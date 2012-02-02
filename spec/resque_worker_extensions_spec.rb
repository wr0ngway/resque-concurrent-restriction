require 'spec_helper'

describe Resque::Plugins::ConcurrentRestriction::Worker do
  include PerformJob

  before(:each) do
    Resque.redis.flushall
  end

  after(:each) do
    Resque.redis.lrange("failed", 0, -1).size.should == 0
    Resque.redis.get("stat:failed").to_i.should == 0
  end

  it "should do nothing for no jobs" do
    run_resque_queue('*')
    Resque.redis.keys("restriction:*").should == []
  end
  
  it "should run normal job without restriction" do
    run_resque_job(NoRestrictionJob, :queue => :normal, :inline => true)
    Resque.size(:normal).should == 0
    NoRestrictionJob.run_count.should == 1
    Resque.redis.keys("restriction:*").should == []
  end
  
  it "should run a restricted job that is not currently restricted" do
    run_resque_job(RestrictionJob, :queue => :normal)
    Resque.size(:normal).should == 0
    RestrictionJob.run_count.should == 1
    RestrictionJob.running_count(RestrictionJob.tracking_key).should == 0
  end
  
  it "should stash a restricted job that is currently restricted" do
    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 99)

    run_resque_job(RestrictionJob, :queue => :normal)

    Resque.size(:normal).should == 0
    RestrictionJob.run_count.should == 0
    RestrictionJob.next_runnable_job(:normal).should be_nil

    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 0)
    RestrictionJob.next_runnable_job(:normal).should == Resque::Job.new('normal', {'class' => 'RestrictionJob', 'args' => []})
  end

  it "should pull job from restricted queue if nothing to run" do
    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 99)

    run_resque_job(RestrictionJob, :queue => :normal)
    RestrictionJob.run_count.should == 0

    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 0)
    RestrictionJob.restriction_queue(RestrictionJob.tracking_key, :normal).should_not == []

    run_resque_queue(:normal)
    RestrictionJob.next_runnable_job(:normal).should be_nil
    RestrictionJob.run_count.should == 1
  end

  it "should prefer running a normal job over one on restriction queue" do
    Resque::Plugins::ConcurrentRestriction.restricted_before_queued.should == false
    
    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 99)

    run_resque_job(RestrictionJob, :queue => :normal)
    RestrictionJob.run_count.should == 0

    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 0)

    run_resque_job(NoRestrictionJob, :queue => :normal)
    RestrictionJob.restriction_queue(RestrictionJob.tracking_key, :normal).should_not == []
    NoRestrictionJob.run_count.should == 1
    RestrictionJob.run_count.should == 0

    run_resque_queue(:normal)
    RestrictionJob.restriction_queue(RestrictionJob.tracking_key, :normal).should == []
    NoRestrictionJob.run_count.should == 1
    RestrictionJob.run_count.should == 1

  end

  it "should prefer running a restricted job over normal one when option given" do
    begin
      Resque::Plugins::ConcurrentRestriction.restricted_before_queued = true

      RestrictionJob.set_running_count(RestrictionJob.tracking_key, 99)

      run_resque_job(RestrictionJob, :queue => :normal)
      RestrictionJob.run_count.should == 0

      RestrictionJob.set_running_count(RestrictionJob.tracking_key, 0)
      
      run_resque_job(NoRestrictionJob, :queue => :normal)
      RestrictionJob.restriction_queue(RestrictionJob.tracking_key, :normal).should == []
      NoRestrictionJob.run_count.should == 0
      RestrictionJob.run_count.should == 1

      run_resque_queue(:normal)
      RestrictionJob.restriction_queue(RestrictionJob.tracking_key, :normal).should == []
      NoRestrictionJob.run_count.should == 1
      RestrictionJob.run_count.should == 1
    ensure
      Resque::Plugins::ConcurrentRestriction.restricted_before_queued = false
    end
  end

  it "should be able to run multiple restricted jobs in a row without exceeding restriction" do
    run_resque_job(RestrictionJob, :queue => :normal)
    run_resque_job(RestrictionJob, :queue => :normal)
    RestrictionJob.run_count.should == 2
  end

  it "should be able to run more restricted jobs than limit in a row" do
    7.times {|i| Resque.enqueue(RestrictionJob, i) }
    7.times {|i| run_resque_queue(:normal) }
    RestrictionJob.total_run_count.should == 7
  end

  it "should preserve queue in restricted job on restriction queue" do
    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 99)

    run_resque_job(RestrictionJob, :queue => :some_queue)

    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 0)

    run_resque_queue(:normal)
    RestrictionJob.run_count.should == 0

    run_resque_queue('some_queue')
    RestrictionJob.run_count.should == 1
  end

  it "should track how many jobs are currently running" do
    t = Thread.new do
      run_resque_job(ConcurrentRestrictionJob)
    end
    sleep 0.1
    ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 1
    t.join
    ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
  end

  it "should run multiple jobs concurrently" do
    7.times {|i| Resque.enqueue(MultipleConcurrentRestrictionJob, i) }
  
    7.times do
       unless child = fork
         Resque.redis.client.connect
         run_resque_queue('*')
         exit!
       end
    end
    sleep 0.25
  
    MultipleConcurrentRestrictionJob.total_run_count.should == 4
    MultipleConcurrentRestrictionJob.running_count(MultipleConcurrentRestrictionJob.tracking_key).should == 4
    MultipleConcurrentRestrictionJob.restriction_queue(MultipleConcurrentRestrictionJob.tracking_key, :normal).size.should == 3
  
    Process.waitall
  
    3.times do
       unless child = fork
         Resque.redis.client.connect
         run_resque_queue('*')
         exit!
       end
    end
    sleep 0.25
  
    MultipleConcurrentRestrictionJob.total_run_count.should == 7
    MultipleConcurrentRestrictionJob.running_count(MultipleConcurrentRestrictionJob.tracking_key).should == 3
    MultipleConcurrentRestrictionJob.restriction_queue(MultipleConcurrentRestrictionJob.tracking_key, :normal).size.should == 0
  
    Process.waitall
  
    MultipleConcurrentRestrictionJob.running_count(MultipleConcurrentRestrictionJob.tracking_key).should == 0
    MultipleConcurrentRestrictionJob.total_run_count.should == 7
  end

  it "should decrement execution number when concurrent job fails" do
    run_resque_job(ConcurrentRestrictionJob, "bad")
    Resque.redis.lrange("failed", 0, -1).size.should == 1
    ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
    Resque.redis.del("failed")
    Resque.redis.del("stat:failed")
  end

  it "should handle jobs with custom restriction identifier" do
    IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 99)

    run_resque_job(IdentifiedRestrictionJob, 1, :queue => :normal)
    run_resque_job(IdentifiedRestrictionJob, 2, :queue => :normal)
    IdentifiedRestrictionJob.run_count(1).should == 0
    IdentifiedRestrictionJob.run_count(2).should == 1

    IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 0)

    run_resque_queue(:normal)
    IdentifiedRestrictionJob.restriction_queue(IdentifiedRestrictionJob.tracking_key(1), :normal).should == []
    IdentifiedRestrictionJob.run_count(1).should == 1
    IdentifiedRestrictionJob.run_count(2).should == 1
  end

  it "should track queue" do
    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 99)

    run_resque_job(RestrictionJob, 1, :queue => :normal1)
    run_resque_job(RestrictionJob, 2, :queue => :normal1)
    run_resque_job(RestrictionJob, 3, :queue => :normal2)
    RestrictionJob.run_count.should == 0

    RestrictionJob.set_running_count(RestrictionJob.tracking_key, 0)

    run_resque_queue(:normal1)
    RestrictionJob.total_run_count.should == 1

    run_resque_queue(:normal1)
    RestrictionJob.total_run_count.should == 2

    run_resque_queue(:normal2)
    RestrictionJob.total_run_count.should == 3
  end

end

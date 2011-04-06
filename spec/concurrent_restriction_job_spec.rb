require 'spec_helper'

describe Resque::Plugins::ConcurrentRestriction do
  include PerformJob

  before(:each) do
    Resque.redis.flushall
  end

  after(:each) do
    Resque.redis.lrange("failed", 0, -1).size.should == 0
  end

  it "should follow the convention" do
    Resque::Plugin.lint(Resque::Plugins::ConcurrentRestrictionJob)
  end

  context "encode/decode" do

    it "should encode jobs correctly" do
      job = Resque::Job.new("somequeue", {"class" => "RestrictionJob", "args" => [1, 2, 3]})
      ConcurrentRestrictionJob.encode(job).should == '{"queue":"somequeue","payload":{"class":"RestrictionJob","args":[1,2,3]}}'
    end

    it "should decode jobs correctly" do
      ConcurrentRestrictionJob.decode(nil).should == nil

      job = ConcurrentRestrictionJob.decode('{"queue":"somequeue","payload":{"class":"RestrictionJob","args":[1,2,3]}}')
      job.should == Resque::Job.new("somequeue", {"class" => "RestrictionJob", "args" => [1, 2, 3]})
    end

    it "should rountrip encode/decode jobs correctly" do
      job = Resque::Job.new("somequeue", {"class" => "RestrictionJob", "args" => [1, 2, 3]})
      ConcurrentRestrictionJob.decode(ConcurrentRestrictionJob.encode(job)).should == job
    end
    
  end

  context "locking" do

    it "should only acquire one lock at a time" do
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 5).should == true
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 5).should == false
    end

    it "should expire locks after timeout" do
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 1).should == true
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 1).should == false
      sleep 3
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 1).should == true
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 1).should == false
    end

    it "should release locks" do
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 5).should == true
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 5).should == false

      ConcurrentRestrictionJob.release_lock("some_lock_key", Time.now.to_i + 5)
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 5).should == true
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i + 5).should == false
    end

    it "should not release lock if expired" do
      ConcurrentRestrictionJob.acquire_lock("some_lock_key", Time.now.to_i - 5).should == true
      ConcurrentRestrictionJob.release_lock("some_lock_key", Time.now.to_i - 5)
      Resque.redis.get("some_lock_key").should_not be_nil
    end

    it "should do a blocking lock while running atomically" do
      counter = nil

      t1 = Thread.new do
        ConcurrentRestrictionJob.run_atomically("some_lock_key") do
          sleep 0.2
          counter = "first"
        end
      end

      sleep 0.1
      t1.alive?.should == true
      
      t2 = Thread.new do
        ConcurrentRestrictionJob.run_atomically("some_lock_key") do
          t1.alive?.should == false
          counter = "second"
        end
      end
      t2.join

      t1.join
      counter.should == "second"
    end

  end

  context "#helpers" do

    it "should mark a job as runnable" do
      ConcurrentRestrictionJob.runnables.should == []
      ConcurrentRestrictionJob.mark_runnable(true)
      ConcurrentRestrictionJob.runnables.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.mark_runnable(false)
      ConcurrentRestrictionJob.runnables.should == []
    end

    it "should push jobs to the restriction queue" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [3]})
      job4 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [4]})
      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == [job1]
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == [job1, job2]
      ConcurrentRestrictionJob.push_to_restriction_queue(job3, :back)
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == [job1, job2, job3]
      ConcurrentRestrictionJob.push_to_restriction_queue(job4, :front)
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == [job4, job1, job2, job3]
      should raise_exception() do
        ConcurrentRestrictionJob.push_to_restriction_queue(job1, :bad)
      end
    end

    it "should pop jobs from restriction queue" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})

      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      popped = ConcurrentRestrictionJob.pop_from_restriction_queue("somequeue", ConcurrentRestrictionJob.tracking_key)
      popped.should == job1
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == [job2]
    end
    
  end

  context "#stash_if_restricted" do
    
    it "should return false and mark running for job that is not restricted" do
      ConcurrentRestrictionJob.mark_runnable(true)
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.stash_if_restricted(job).should == false
      ConcurrentRestrictionJob.running_count.should == 1
      ConcurrentRestrictionJob.runnables.should == [ConcurrentRestrictionJob.tracking_key]
    end

    it "should return true and not mark running for job that is restricted" do
      Resque.redis.set(ConcurrentRestrictionJob.running_count_key, 99)
      ConcurrentRestrictionJob.mark_runnable(false)
      ConcurrentRestrictionJob.runnables.should == []
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.stash_if_restricted(job).should == true
      ConcurrentRestrictionJob.running_count.should == 99
      ConcurrentRestrictionJob.runnables.should == []
    end

  end

  context "#next_runnable_job" do

    it "should do nothing when nothing runnable" do
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should be_nil
    end

    it "should not get a job if nothing runnable" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should be_nil
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == [job1]
    end

    it "should get a job if something runnable" do
      ConcurrentRestrictionJob.mark_runnable(true)
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should == job1
      ConcurrentRestrictionJob.restriction_queue("somequeue").should == []
    end

    it "should not get a job if something runnable on other queue" do
      ConcurrentRestrictionJob.mark_runnable(true)
      job1 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should be_nil
      ConcurrentRestrictionJob.restriction_queue("somequeue2").should == [job1]
    end

  end

  context "#release_restriction" do

    it "should release restriction" do
      ConcurrentRestrictionJob.mark_runnable(false)
      Resque.redis.set(ConcurrentRestrictionJob.running_count_key, 1)
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.release_restriction(job)
      ConcurrentRestrictionJob.running_count.should == 0
      ConcurrentRestrictionJob.runnables.should == [ConcurrentRestrictionJob.tracking_key]
    end

    it "should mark not runnable if it erroneously was" do
      ConcurrentRestrictionJob.mark_runnable(true)
      Resque.redis.set(ConcurrentRestrictionJob.running_count_key, 2)
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.release_restriction(job)
      ConcurrentRestrictionJob.running_count.should == 1
      ConcurrentRestrictionJob.runnables.should == []
    end

    it "should keep restriction above 0" do
      Resque.redis.set(ConcurrentRestrictionJob.running_count_key, 0)
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.release_restriction(job)
      ConcurrentRestrictionJob.running_count.should == 0
      ConcurrentRestrictionJob.runnables.should == [ConcurrentRestrictionJob.tracking_key]
    end

  end

  context "#stats" do
    
    it "should have blank info when nothing going on" do
      stats = ConcurrentRestrictionJob.stats
      stats[:queue_totals][:by_queue_name].should == {}
      stats[:queue_totals][:by_identifier].should == {}
      stats[:running_counts].should == {}
      stats[:lock_count].should == 0
      stats[:runnable_count].should == 0
    end

    it "should track queue_totals" do
      job1 = Resque::Job.new("queue1", {"class" => "IdentifiedRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("queue2", {"class" => "IdentifiedRestrictionJob", "args" => [1]})
      job3 = Resque::Job.new("queue1", {"class" => "IdentifiedRestrictionJob", "args" => [2]})
      job4 = Resque::Job.new("queue2", {"class" => "IdentifiedRestrictionJob", "args" => [2]})
      job5 = Resque::Job.new("queue3", {"class" => "ConcurrentRestrictionJob", "args" => []})

      IdentifiedRestrictionJob.push_to_restriction_queue(job1)
      IdentifiedRestrictionJob.push_to_restriction_queue(job2)
      IdentifiedRestrictionJob.push_to_restriction_queue(job3)
      IdentifiedRestrictionJob.push_to_restriction_queue(job4)
      ConcurrentRestrictionJob.push_to_restriction_queue(job5)

      stats = IdentifiedRestrictionJob.stats
      stats[:queue_totals][:by_queue_name].should == {"queue1" => 2, "queue2" => 2, "queue3" => 1}
      stats[:queue_totals][:by_identifier].should == {"IdentifiedRestrictionJob:1" => 2, "IdentifiedRestrictionJob:2" => 2, "ConcurrentRestrictionJob" => 1}
    end

    it "should track running_counts" do
      Resque.redis.set(IdentifiedRestrictionJob.running_count_key(1), 2)
      Resque.redis.set(IdentifiedRestrictionJob.running_count_key(2), 3)
      Resque.redis.set(ConcurrentRestrictionJob.running_count_key, 4)
      stats = IdentifiedRestrictionJob.stats
      stats[:running_counts].should == {"IdentifiedRestrictionJob:1" => 2, "IdentifiedRestrictionJob:2" => 3, "ConcurrentRestrictionJob" => 4}
    end

    it "should track lock_count" do
      IdentifiedRestrictionJob.acquire_lock(IdentifiedRestrictionJob.lock_key(IdentifiedRestrictionJob.tracking_key(1)), Time.now.to_i)
      IdentifiedRestrictionJob.acquire_lock(IdentifiedRestrictionJob.lock_key(IdentifiedRestrictionJob.tracking_key(2)), Time.now.to_i)
      ConcurrentRestrictionJob.acquire_lock(ConcurrentRestrictionJob.lock_key(ConcurrentRestrictionJob.tracking_key), Time.now.to_i)
      stats = IdentifiedRestrictionJob.stats
      stats[:lock_count].should == 3
    end

    it "should track runnable_count" do
      ConcurrentRestrictionJob.mark_runnable(true)
      IdentifiedRestrictionJob.mark_runnable(true)
      stats = IdentifiedRestrictionJob.stats
      stats[:runnable_count].should == 2
    end

  end

end

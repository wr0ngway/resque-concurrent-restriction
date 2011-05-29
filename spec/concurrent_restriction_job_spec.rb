require 'spec_helper'

describe Resque::Plugins::ConcurrentRestriction do
  include PerformJob

  before(:each) do
    Resque.redis.flushall
  end

  after(:each) do
    Resque.redis.lrange("failed", 0, -1).size.should == 0
    Resque.redis.get("stat:failed").to_i.should == 0
  end

  it "should follow the convention" do
    Resque::Plugin.lint(Resque::Plugins::ConcurrentRestrictionJob)
  end

  context "settings" do

    it "should allow setting/getting global config" do
      Resque::Plugins::ConcurrentRestriction.lock_timeout.should == 60
      Resque::Plugins::ConcurrentRestriction.configure do |config|
        config.lock_timeout = 61
      end
      Resque::Plugins::ConcurrentRestriction.lock_timeout.should == 61
      Resque::Plugins::ConcurrentRestriction.lock_timeout = 60
      Resque::Plugins::ConcurrentRestriction.lock_timeout.should == 60
    end

  end

  context "keys" do
    it "should always contain the classname in tracking_key" do
      ConcurrentRestrictionJob.tracking_key.should == "concurrent.tracking.ConcurrentRestrictionJob"
      IdentifiedRestrictionJob.tracking_key.should == "concurrent.tracking.IdentifiedRestrictionJob"
      IdentifiedRestrictionJob.tracking_key(1).should == "concurrent.tracking.IdentifiedRestrictionJob.1"
      Jobs::NestedRestrictionJob.tracking_key.should == "concurrent.tracking.Jobs::NestedRestrictionJob"
    end

    it "should be able to get the class from tracking_key" do
      ConcurrentRestrictionJob.tracking_class(ConcurrentRestrictionJob.tracking_key).should == ConcurrentRestrictionJob
      IdentifiedRestrictionJob.tracking_class(IdentifiedRestrictionJob.tracking_key).should == IdentifiedRestrictionJob
      IdentifiedRestrictionJob.tracking_class(IdentifiedRestrictionJob.tracking_key(1)).should == IdentifiedRestrictionJob
      Jobs::NestedRestrictionJob.tracking_class(Jobs::NestedRestrictionJob.tracking_key).should == Jobs::NestedRestrictionJob
    end
    
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
      IdentifiedRestrictionJob.runnables.should == []
      IdentifiedRestrictionJob.runnables(:somequeue).should == []
      IdentifiedRestrictionJob.runnables(:somequeue2).should == []

      IdentifiedRestrictionJob.update_queues_available(IdentifiedRestrictionJob.tracking_key(1), :somequeue, :add)
      IdentifiedRestrictionJob.update_queues_available(IdentifiedRestrictionJob.tracking_key(2), :somequeue2, :add)

      IdentifiedRestrictionJob.mark_runnable(IdentifiedRestrictionJob.tracking_key(1), true)
      IdentifiedRestrictionJob.runnables(:somequeue).should == [IdentifiedRestrictionJob.tracking_key(1)]
      IdentifiedRestrictionJob.runnables(:somequeue2).should == []
      IdentifiedRestrictionJob.runnables.should == [IdentifiedRestrictionJob.tracking_key(1)]

      IdentifiedRestrictionJob.mark_runnable(IdentifiedRestrictionJob.tracking_key(2), true)
      IdentifiedRestrictionJob.runnables(:somequeue).should == [IdentifiedRestrictionJob.tracking_key(1)]
      IdentifiedRestrictionJob.runnables(:somequeue2).should == [IdentifiedRestrictionJob.tracking_key(2)]
      IdentifiedRestrictionJob.runnables.sort.should == [IdentifiedRestrictionJob.tracking_key(1), IdentifiedRestrictionJob.tracking_key(2)].sort

      IdentifiedRestrictionJob.mark_runnable(IdentifiedRestrictionJob.tracking_key(1), false)
      IdentifiedRestrictionJob.runnables(:somequeue).should == []
      IdentifiedRestrictionJob.runnables(:somequeue2).should == [IdentifiedRestrictionJob.tracking_key(2)]
      IdentifiedRestrictionJob.runnables.should == [IdentifiedRestrictionJob.tracking_key(2)]

      IdentifiedRestrictionJob.mark_runnable(IdentifiedRestrictionJob.tracking_key(2), false)
      IdentifiedRestrictionJob.runnables(:somequeue).should == []
      IdentifiedRestrictionJob.runnables(:somequeue2).should == []
      IdentifiedRestrictionJob.runnables.should == []
    end

  end

  context "running count" do

    it "should set running count" do
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 5)
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 5
    end

    it "should increment running count" do
      ConcurrentRestrictionJob.stub!(:concurrent_limit).and_return(1)
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
      ConcurrentRestrictionJob.increment_running_count(ConcurrentRestrictionJob.tracking_key).should == false
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 1
      ConcurrentRestrictionJob.increment_running_count(ConcurrentRestrictionJob.tracking_key).should == true
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 2
    end

    it "should decrement running count" do
      ConcurrentRestrictionJob.stub!(:concurrent_limit).and_return(1)
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 2)
      ConcurrentRestrictionJob.decrement_running_count(ConcurrentRestrictionJob.tracking_key).should == true
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 1
      ConcurrentRestrictionJob.decrement_running_count(ConcurrentRestrictionJob.tracking_key).should == false
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
      ConcurrentRestrictionJob.decrement_running_count(ConcurrentRestrictionJob.tracking_key).should == false
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
      ConcurrentRestrictionJob.decrement_running_count(ConcurrentRestrictionJob.tracking_key).should == false
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
    end

    it "should be able to tell when restricted" do
      ConcurrentRestrictionJob.stub!(:concurrent_limit).and_return(1)
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 0)
      ConcurrentRestrictionJob.restricted?(ConcurrentRestrictionJob.tracking_key).should == false
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 1)
      ConcurrentRestrictionJob.restricted?(ConcurrentRestrictionJob.tracking_key).should == true
    end

  end

  context "restriction queue" do

    it "should push jobs to the restriction queue" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [3]})
      job4 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [4]})
      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == [job1]
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == [job1, job2]
      ConcurrentRestrictionJob.push_to_restriction_queue(job3, :back)
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == [job1, job2, job3]
      ConcurrentRestrictionJob.push_to_restriction_queue(job4, :front)
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == [job4, job1, job2, job3]
      should raise_exception() do
        ConcurrentRestrictionJob.push_to_restriction_queue(job1, :bad)
      end
    end

    it "should pop jobs from restriction queue" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})

      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      popped = ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      popped.should == job1
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == [job2]
    end

    it "should add to queue availabilty on push" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})

      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue"]

      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue"]

      ConcurrentRestrictionJob.push_to_restriction_queue(job3)
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue", "somequeue2"]
    end

    it "should clean up queue availabilty on pop" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})
      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.push_to_restriction_queue(job3)

      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue", "somequeue2"]

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue2")
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue"]

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue"]

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == []
    end

    it "should ensure runnables on queue when pushed" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})


      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.push_to_restriction_queue(job3)

      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key(1), 0)

      ConcurrentRestrictionJob.runnables.sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue").sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue2").sort.should == [ConcurrentRestrictionJob.tracking_key]

    end

    it "should remove runnables for queues on pop" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})

      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.push_to_restriction_queue(job3)
      ConcurrentRestrictionJob.stub!(:concurrent_limit).and_return(5)

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      ConcurrentRestrictionJob.runnables.sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue").sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue2").sort.should == [ConcurrentRestrictionJob.tracking_key]

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      ConcurrentRestrictionJob.runnables.sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue").sort.should == []
      ConcurrentRestrictionJob.runnables("somequeue2").sort.should == [ConcurrentRestrictionJob.tracking_key]

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue2")
      ConcurrentRestrictionJob.runnables.sort.should == []
      ConcurrentRestrictionJob.runnables("somequeue").sort.should == []
      ConcurrentRestrictionJob.runnables("somequeue2").sort.should == []
    end

    it "should track queue_counts" do
      ConcurrentRestrictionJob.queue_counts.should == {}

      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})

      ConcurrentRestrictionJob.push_to_restriction_queue(job1)
      ConcurrentRestrictionJob.push_to_restriction_queue(job2)
      ConcurrentRestrictionJob.push_to_restriction_queue(job3)
      ConcurrentRestrictionJob.queue_counts.should == {"somequeue"=>2, "somequeue2"=>1}

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      ConcurrentRestrictionJob.queue_counts.should == {"somequeue"=>1, "somequeue2"=>1}

      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue")
      ConcurrentRestrictionJob.pop_from_restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue2")
      ConcurrentRestrictionJob.queue_counts.should == {"somequeue"=>0, "somequeue2"=>0}
    end
  end

  context "#stash_if_restricted" do
    
    it "should return false and mark running for job that is not restricted" do
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.stash_if_restricted(job).should == false
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 1
    end

    it "should return true and not mark running for job that is restricted" do
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key(1), 99)

      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.stash_if_restricted(job).should == true
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 99
      ConcurrentRestrictionJob.runnables.should == []
    end

    it "should add to queue availabilty on stash when restricted" do
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key(1), 99)

      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})

      ConcurrentRestrictionJob.stash_if_restricted(job1)
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue"]

      ConcurrentRestrictionJob.stash_if_restricted(job2)
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue"]

      ConcurrentRestrictionJob.stash_if_restricted(job3)
      ConcurrentRestrictionJob.queues_available(ConcurrentRestrictionJob.tracking_key).sort.should == ["somequeue", "somequeue2"]
    end

    it "should map available queues to tracking key on push" do
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 99)

      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [1]})
      job2 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => [2]})
      job3 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => [3]})

      ConcurrentRestrictionJob.stash_if_restricted(job1)
      ConcurrentRestrictionJob.stash_if_restricted(job3)
      ConcurrentRestrictionJob.stash_if_restricted(job2)

      ConcurrentRestrictionJob.runnables.sort.should == []
      ConcurrentRestrictionJob.runnables("somequeue").sort.should == []
      ConcurrentRestrictionJob.runnables("somequeue2").sort.should == []

      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 0)

      ConcurrentRestrictionJob.runnables.sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue").sort.should == [ConcurrentRestrictionJob.tracking_key]
      ConcurrentRestrictionJob.runnables("somequeue2").sort.should == [ConcurrentRestrictionJob.tracking_key]

    end

  end

  context "#next_runnable_job" do

    it "should do nothing when nothing runnable" do
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should be_nil
    end

    it "should not get a job if nothing runnable" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 99)
      ConcurrentRestrictionJob.stash_if_restricted(job1)
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should be_nil
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == [job1]
    end

    it "should get a job if something runnable" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 99)
      ConcurrentRestrictionJob.stash_if_restricted(job1)

      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 0)
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should == job1
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue").should == []
    end

    it "should not get a job if something runnable on other queue" do
      job1 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 99)
      ConcurrentRestrictionJob.stash_if_restricted(job1)
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 0)

      ConcurrentRestrictionJob.next_runnable_job('somequeue').should be_nil
      ConcurrentRestrictionJob.restriction_queue(ConcurrentRestrictionJob.tracking_key, "somequeue2").should == [job1]
    end

    it "should get a job for right class when called through ConcurrentRestrictionJob" do
      job1 = Resque::Job.new("somequeue", {"class" => "IdentifiedRestrictionJob", "args" => [1]})
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 99)
      IdentifiedRestrictionJob.stash_if_restricted(job1)
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 0)

      # Use ConcurrentRestrictionJob as thats what Resque::Worker::reserve extension has to use
      ConcurrentRestrictionJob.next_runnable_job('somequeue').should == job1
    end

  end

  context "#release_restriction" do

    it "should decrement running count on release restriction" do
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 1)
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.release_restriction(job)
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
    end

    it "should keep restriction above 0" do
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 0)
      job = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      ConcurrentRestrictionJob.release_restriction(job)
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
    end

  end

  context "#reset_restrictions" do
    it "should do nothing when nothing is in redis" do
      ConcurrentRestrictionJob.reset_restrictions.should == [0, 0]
    end

    it "should reset counts" do
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 5)
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 5)
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(2), 5)

      ConcurrentRestrictionJob.reset_restrictions.should == [3, 0]
      ConcurrentRestrictionJob.running_count(ConcurrentRestrictionJob.tracking_key).should == 0
      IdentifiedRestrictionJob.running_count(IdentifiedRestrictionJob.tracking_key(1)).should == 0
      IdentifiedRestrictionJob.running_count(IdentifiedRestrictionJob.tracking_key(2)).should == 0
    end

    it "should reset restriction queue runnable state" do
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

      ConcurrentRestrictionJob.reset_restrictions.should == [0, 5]
      IdentifiedRestrictionJob.runnables(:queue1).sort.should == [IdentifiedRestrictionJob.tracking_key(1), IdentifiedRestrictionJob.tracking_key(2)]
      IdentifiedRestrictionJob.runnables(:queue2).sort.should == [IdentifiedRestrictionJob.tracking_key(1), IdentifiedRestrictionJob.tracking_key(2)]
      ConcurrentRestrictionJob.runnables(:queue3).sort.should == [ConcurrentRestrictionJob.tracking_key]

      IdentifiedRestrictionJob.runnables.sort.should == [ConcurrentRestrictionJob.tracking_key, IdentifiedRestrictionJob.tracking_key(1), IdentifiedRestrictionJob.tracking_key(2)]

      ConcurrentRestrictionJob.queue_counts.should == {"queue1"=>2, "queue2"=>2, "queue3"=>1}

    end

  end

  context "#stats" do
    
    it "should have blank info when nothing going on" do
      stats = ConcurrentRestrictionJob.stats
      stats[:queues].should == {}
      stats[:identifiers].should == {}
      stats[:lock_count].should == 0
      stats[:runnable_count].should == 0
      estats = ConcurrentRestrictionJob.stats(true)
      estats.should == stats
    end

    it "should not track identifiers when not extended" do
      job1 = Resque::Job.new("queue1", {"class" => "IdentifiedRestrictionJob", "args" => [1]})
      IdentifiedRestrictionJob.push_to_restriction_queue(job1)
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 2)
      
      stats = IdentifiedRestrictionJob.stats
      stats[:identifiers].should == {}
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

      stats = IdentifiedRestrictionJob.stats(true)
      stats[:queues].should == {"queue1" => 2, "queue2" => 2, "queue3" => 1}

      stats[:identifiers].should == {"IdentifiedRestrictionJob.1"=>{"queue1"=>1, "queue2"=>1}, "IdentifiedRestrictionJob.2"=>{"queue1"=>1, "queue2"=>1}, "ConcurrentRestrictionJob"=>{"queue3"=>1}}
    end

    it "should track running_counts" do
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 2)
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(2), 3)
      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 4)
      stats = IdentifiedRestrictionJob.stats(true)
      stats[:identifiers].should == {"IdentifiedRestrictionJob.1"=>{"running"=>2}, "IdentifiedRestrictionJob.2"=>{"running"=>3}, "ConcurrentRestrictionJob"=>{"running"=>4}}
    end

    it "should track lock_count" do
      IdentifiedRestrictionJob.acquire_lock(IdentifiedRestrictionJob.lock_key(IdentifiedRestrictionJob.tracking_key(1)), Time.now.to_i)
      IdentifiedRestrictionJob.acquire_lock(IdentifiedRestrictionJob.lock_key(IdentifiedRestrictionJob.tracking_key(2)), Time.now.to_i)
      ConcurrentRestrictionJob.acquire_lock(ConcurrentRestrictionJob.lock_key(ConcurrentRestrictionJob.tracking_key), Time.now.to_i)
      stats = IdentifiedRestrictionJob.stats
      stats[:lock_count].should == 3
    end

    it "should track runnable_count" do
      job1 = Resque::Job.new("somequeue", {"class" => "ConcurrentRestrictionJob", "args" => []})
      job2 = Resque::Job.new("somequeue2", {"class" => "ConcurrentRestrictionJob", "args" => []})
      job3 = Resque::Job.new("somequeue", {"class" => "IdentifiedRestrictionJob", "args" => [1]})

      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 99)
      ConcurrentRestrictionJob.stash_if_restricted(job1)
      ConcurrentRestrictionJob.stash_if_restricted(job2)

      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 99)
      IdentifiedRestrictionJob.stash_if_restricted(job3)

      ConcurrentRestrictionJob.set_running_count(ConcurrentRestrictionJob.tracking_key, 0)
      IdentifiedRestrictionJob.set_running_count(IdentifiedRestrictionJob.tracking_key(1), 0)

      stats = IdentifiedRestrictionJob.stats
      stats[:runnable_count].should == 2
    end

  end

end

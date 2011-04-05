require 'resque'
require 'resque/plugins/concurrent_restriction/version'
require 'resque/plugins/concurrent_restriction/resque_worker_extension'
require 'resque/plugins/concurrent_restriction/concurrent_restriction_job'

Resque::Worker.send(:include, Resque::Plugins::ConcurrentRestriction::Worker)
Resque::Job.send(:extend, Resque::Plugins::ConcurrentRestriction::Job)

unsupported_version = false
begin
  server_ver = Resque.redis.info["redis_version"].split('.').collect{|x| x.to_i}
  unsupported_version = (server_ver <=> [2, 2, 0]) < 0
rescue
end

raise "resque-concurrent-restriction requires a redis-server version >= 2.2.0" if unsupported_version

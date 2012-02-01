# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require 'resque/plugins/concurrent_restriction/version'


Gem::Specification.new do |s|
  s.name        = "resque-concurrent-restriction"
  s.version     = Resque::Plugins::ConcurrentRestriction::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Matt Conway"]
  s.email       = ["matt@conwaysplace.com"]
  s.homepage    = "http://github.com/wr0ngway/resque-concurrent-restriction"
  s.summary     = %q{A resque plugin for limiting how many of a specific job can run concurrently}
  s.description = %q{A resque plugin for limiting how many of a specific job can run concurrently}

  s.rubyforge_project = "resque-concurrent-restriction"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency("resque", '~> 1.10')

  s.add_development_dependency('json')
  s.add_development_dependency('rspec', '~> 2.5')
  s.add_development_dependency('awesome_print')

  # Needed for testing newer resque on ruby 1.8.7
  s.add_development_dependency('json')

end

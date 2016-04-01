# resque-concurrent-restriction

Resque Concurrent Restriction is a plugin for the [Resque][0] queueing system. It allows one to specify how many of the given job can run concurrently.

Resque Concurrent Restriction requires Resque 1.25 and redis 2.2

[![Build Status](https://circleci.com/gh/russCloak/resque-concurrent-restriction.png?style=shield)](http://travis-ci.org/russCloak/resque-concurrent-restriction)

## Install

Install it from the command-line:

```bash
gem install resque-concurrent-restriction
```

or add it to your `Gemfile`:

```ruby
gem 'resque-concurrent-restriction', '~> 0.6'
```

## To use

It is especially useful when a system has intensive jobs for which you should only run a few at a time. What you should do for the IntensiveJob is to make it extend Resque::Plugins::ConcurrentRestriction and specify the concurrent limit (defaults to 1). For example:

```ruby
class IntensiveJob
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 4

  def perform
    # ...
  end
end
```

That means the IntensiveJob can not have more than 4 jobs running simultaneously.

One can also make the concurrency limit depend on the parameters of a job. For example, if you always pass a user_id as the first parameter, you can restrict the job to run N concurrent jobs per user:

```ruby
class IntensiveJob
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 4

  def self.concurrent_identifier(*args)
    args.first.to_s
  end

  def perform
    # ...
  end
end
```

## Author

Code was originally forked from the [resque-restriction][1] plugin (Richard Huang :: flyerhzm@gmail.com :: @flyerhzm), but diverged enough that it warranted being its own plugin to keep the code simple.

Matt Conway :: matt@conwaysplace.com :: @mattconway

## Copyright

Copyright (c) 2011 Matt Conway. See LICENSE for details.

[0]: http://github.com/defunkt/resque
[1]: http://github.com/flyerhzm/resque-restriction


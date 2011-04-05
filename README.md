resque-concurrent-restriction
===============

Resque Concurrent Restriction is a plugin for the [Resque][0] queueing system (http://github.com/defunkt/resque). It allows one to specify how many of the given job can run concurrently.

Resque Concurrent Restriction requires Resque 1.10 and redis 2.2

Install
-------

  sudo gem install resque-concurrent-restriction

To use
------

It is especially useful when a system has intensive jobs for which you should only run a few at a time.  What you should do for the IntensiveJob is to make it extend Resque::Plugins::ConcurrentRestriction and specify the concurrent limit (defaults to 1). For example:

	class IntensiveJob
	  extend Resque::Plugins::ConcurrentRestriction
	  concurrent 4

	  #rest of your class here
	end

That means the IntensiveJob can not have more than 4 jobs running simultaneously

Author
------
Code was originally forkd from the [resque-restriction][1] plugin (Richard Huang :: flyerhzm@gmail.com :: @flyerhzm), but diverged enough that it warranted being its own plugin to keep the code simple.

Matt Conway :: matt@conwaysplace.com :: @mattconway

Copyright
---------
Copyright (c) 2011 Matt Conway. See LICENSE for details.

[0]: http://github.com/defunkt/resque
[1]: http://github.com/flyerhzm/resque-restriction


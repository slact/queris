# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "queris/version"

Gem::Specification.new do |s|
  s.name        = "queris"
  s.version     = Queris::VERSION
  s.authors     = ["Leo P."]
  s.email       = ["queris@slact.net"]
  s.homepage    = ""
  s.summary     = %q{Redis-backed object indexing and querying module}
  s.description = %q{We've got indices, foreign indices, ranges, arbitrary numbers of subqueries, and more}

  s.rubyforge_project = "queris"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  # s.add_development_dependency "rspec"
  # s.add_runtime_dependency "rest-client"
end

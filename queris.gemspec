
# -*- encoding: utf-8 -*-
$:.push('lib')
require "queris/version"

Gem::Specification.new do |s|
  s.name        = "queris"
  s.version     = Queris::VERSION
  s.authors     = ["Leo P."]
  s.email       = ["queris@slact.net"]
  s.homepage    = ""
  s.summary     = "Redis-backed object indexing and querying module"
  s.description = "We've got indices, foreign indices, ranges, subqueries, realtime queries, "

  dependencies = [
    # Examples:
    [:runtime, "redis", "~> 5.2.0", :require => ["redis", "hiredis-client"]],
    [:runtime, "hiredis-client"],
    [:development, "pry"],
    [:development, "pry-debundle"]
  ]
  
  s.files         = Dir['**/*']
  s.test_files    = Dir['test/**/*'] + Dir['spec/**/*']
  s.executables   = Dir['bin/*'].map { |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  
  ## Make sure you can build the gem on older versions of RubyGems too:
  s.rubygems_version = "2.4.5"
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.specification_version = 3 if s.respond_to? :specification_version
  
  dependencies.each do |type, name, version|
    if s.respond_to?("add_#{type}_dependency")
      s.send("add_#{type}_dependency", name, version)
    else
      s.add_dependency(name, version)
    end
  end
end

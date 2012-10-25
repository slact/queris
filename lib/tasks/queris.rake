#!ruby
namespace :queris do
  def confirm
    if $stdin.respond_to? 'getch'
      $stdin.getch.upcase == "Y"
    else #ruby <= 1.9.2 doesn't have getch
      $stdin.readline[0].upcase == "Y"
    end
  end
  def warn(action=nil, warning=nil, times=1)
    puts warning if warning
    if action then
      q = "Do you #{times>1 ? 'really ' : ''}want to #{action}?"
    else
      if times == 1
        q = "Did you back up everything you needed to back up?"
      else
        q = "Are you sure you want to proceed?"
      end
    end
    puts "#{q} [y/n]"
    if confirm
      if times <= 1
        return true
      else
        return warn(nil, nil, times-1)
      end
    end
  end

  def build_index(index, check_existence=true, incremental=false)
    if check_existence
      model = index.model
      print "Checking if index #{index.name} already exists..." 
      foundkeys = index.respond_to?('keypattern') ? model.redis.keys(index.keypattern) : []
      if foundkeys.count > 0
        puts "it does."
        if incremental
          puts "#{model.name} #{index.name} index data will be deleted incrementally, per element. This runs safely on live data."
        else
          puts "All #{model.name} #{index.name} index data will be deleted. Make sure you have a backup!"
        end
        return false unless warn
        if incremental
          puts "This index spans #{foundkeys.count} redis keys. Every element must be deleted from each of those keys. This may take a while and will slow down redis."
          if foundkeys.count > 500
            return false unless warn "continue"
          end
        end
      else
        puts "it doesn't."
      end
    end
    puts "Building index #{index.name} for #{model.name}"
    model.build_redis_index index.name, incremental
  end
  
  def load_models
    # Load all the application's models. Courtesy of random patch for Sunspot ()
    Rails.root.join('app', 'models').tap do |models_path|
      Dir.glob(models_path.join('**', '*.rb')).map do |path|
        ActiveSupport::Dependencies.require_or_load path.sub(models_path.to_s+'/', '')[0..-4] rescue nil
      end.compact
    end
  end
  desc "Rebuild all queris indices, optionally deleting nearly everything beforehand"
  task :rebuild, [:clear] => :environment do |t, args|
    args.with_defaults(:clear => false)
    abort unless warn "rebuild all redis indices", "All current redis indices and queries will be destroyed!", 3
    load_models
    Queris.rebuild!(args.clear)
  end

  desc "Build all missing indices or a given redis index in the given model"
  task :'build', [:model, :index, :incremental] => :environment do |t, args|
    load_models
    #abort "Please specify a model." if args.model.nil?
    #abort "Please specify an index." if args.index.nil?
    if args.model && args.model.length > 0 then
      begin
        model = Object.const_get args.model
        models = [ model ]
      rescue NameError
        abort "No model #{args.model} found."
      end
    else
      models = Queris.models
    end

    if args.index
      begin 
        index = model.redis_index args.index
      rescue
        abort "No index named #{args.index} found in #{model.name}."
      end
    end
    
    if model and index then
      #just one index to build
      build_index index, true, !!args.incremental
    else
      models.each do |model|
        missing = []
        model.redis_indices.each do |i|
          missing << i unless i.skip_create? || i.exists?
        end
        if missing.count > 0
          puts "#{model.name} is missing #{missing.count} #{missing.count  == 1 ? 'index' : 'indices'}: #{missing.map(&:name).join(', ')}."
          model.build_redis_indices missing
        else
          puts "#{model.name} indices already built."
        end
      end
    end
  end 
  
  desc "Clear all object caches"
  task :clear_cache => :environment do
    load_models
    puts "Deleted #{Queris.clear_cache!} cache keys."
  end
  
  desc "Clear all queries"
  task :clear_queries => :environment do
    load_models
    abort unless warn "clear all queries", "All queries, live and otherwise, and all metaqueries will be deleted"
    puts "Deleted #{Queris.clear_queries!} query keys."
  end
  
  desc "Clear all caches and queries"
  task :clear => :environment do
    load_models
    abort unless warn "clear all queries and caches", "All caches and queries will be deleted", 2
    puts "Deleted #{Queris.clear!} keys."
  end

  desc "Queris data sumaries"
  task :info => :environment do
    load_models
    Queris.info
  end
end

namespace :queris do
  def confirm
    $stdin.getch.upcase=='Y'
  end
  def warn(action, warning, times=1)
    puts warning if warning
    if action then
      q = "Do you really want to #{action}?"
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
    abort unless warn "rebuild all redis indices", "All current redis indices and queries will be destroyed!", 2
    load_models
    Queris.rebuild!(args.clear)
  end

  desc "Build a redis index in the given model"
  task :'build', [:model, :index] => :environment do |t, args|
    load_models
    begin
      model = Object.const_get args.model
    rescue NameError
      abort "No model #{args.model} found."
    end
    begin 
      index = model.redis_index args.index
    rescue
      abort "No index named #{args.index} found in #{model.name}."
    end
    puts "Building index #{index.name} for #{model.name}"
    model.build_redis_index index.name
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
end

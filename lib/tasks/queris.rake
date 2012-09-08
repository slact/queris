namespace :queris do
  def confirm
    $stdin.getch.upcase=='Y'
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
    warning = "Are you sure you want to rebuild all redis indices?"
    warning += " All current redis indices and queries will be destroyed!" if args.clear
    puts warning + " [y/n]"
    abort unless confirm
    puts "Did you back up everything you needed to back up? [y/n]"
    abort unless confirm
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

  desc "Garbage-collect expired live queries from QueryStore"
  task :gc => :environment do
    load_models
    unless Queris.const_defined? 'QueryStore'
      abort "No live queries in any of your models, nothing to do."
    end
    Queris::QueryStore.gc
  end
end

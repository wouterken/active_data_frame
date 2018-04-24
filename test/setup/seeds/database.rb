db_config       = YAML::load(File.open(File.expand_path('../../database.yml', __FILE__)))
db_config_admin = db_config.merge({'database' => 'postgres', 'schema_search_path' => 'public'})

ActiveRecord::Base.establish_connection(db_config_admin)
ActiveRecord::Base.connection.drop_database(db_config["database"])
ActiveRecord::Base.connection.create_database(db_config["database"])
ActiveRecord::Base.establish_connection(db_config)
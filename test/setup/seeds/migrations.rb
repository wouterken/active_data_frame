
class CreateAirports < ActiveRecord::Migration[5.0]
  def change
    create_table :airports do |t|
      t.string :name
      t.string :code
      t.string :iata
      t.string :country
      t.timestamps
    end
  end
end

class CreateDepartures < ActiveRecord::Migration[5.0]
  def change
    create_table :departure_blocks do |t|
      t.integer :data_frame_id, index: true
      t.string  :data_frame_type, index: true
      t.integer :period_index, index: true
      t.integer :t1
      t.integer :t2
      t.integer :t3
      t.integer :t4
      t.integer :t5
      t.integer :t6
      t.integer :t7
      t.integer :t8
      t.integer :t9
      t.integer :t10
      t.integer :t11
      t.integer :t12
      t.integer :t13
      t.integer :t14
      t.integer :t15
      t.integer :t16
      t.integer :t17
      t.integer :t18
      t.integer :t19
      t.integer :t20
      t.integer :t21
      t.integer :t22
      t.integer :t23
      t.integer :t24
    end

    add_index :departure_blocks, [:data_frame_id , :period_index, :data_frame_type], :unique => true, name: 'index_departure_blocks_on_type_id_and_index'
  end
end

class CreateArrivals < ActiveRecord::Migration[5.0]
  def change
    create_table :arrival_blocks do |t|
      t.integer :data_frame_id, index: true
      t.string  :data_frame_type, index: true
      t.integer :period_index, index: true
      t.integer :t1
      t.integer :t2
      t.integer :t3
      t.integer :t4
      t.integer :t5
      t.integer :t6
      t.integer :t7
      t.integer :t8
      t.integer :t9
      t.integer :t10
      t.integer :t11
      t.integer :t12
      t.integer :t13
      t.integer :t14
      t.integer :t15
      t.integer :t16
      t.integer :t17
      t.integer :t18
      t.integer :t19
      t.integer :t20
      t.integer :t21
      t.integer :t22
      t.integer :t23
      t.integer :t24
    end

    add_index :arrival_blocks, [:data_frame_id , :period_index, :data_frame_type], :unique => true, name: 'index_arrival_blocks_on_type_id_and_index'
  end
end

class CreateTemperatures < ActiveRecord::Migration[5.0]
  def change
    create_table :temperature_blocks do |t|
      t.integer :data_frame_id, index: true
      t.string  :data_frame_type, index: true
      t.integer :period_index, index: true
      t.float :t1,  precision: 53
      t.float :t2,  precision: 53
      t.float :t3,  precision: 53
      t.float :t4,  precision: 53
      t.float :t5,  precision: 53
      t.float :t6,  precision: 53
      t.float :t7,  precision: 53
      t.float :t8,  precision: 53
      t.float :t9,  precision: 53
      t.float :t10, precision: 53
      t.float :t11, precision: 53
      t.float :t12, precision: 53
      t.float :t13, precision: 53
      t.float :t14, precision: 53
      t.float :t15, precision: 53
      t.float :t16, precision: 53
      t.float :t17, precision: 53
      t.float :t18, precision: 53
      t.float :t19, precision: 53
      t.float :t20, precision: 53
      t.float :t21, precision: 53
      t.float :t22, precision: 53
      t.float :t23, precision: 53
      t.float :t24, precision: 53
    end

    add_index :temperature_blocks, [:data_frame_id , :period_index, :data_frame_type], :unique => true, name: 'index_temperature_blocks_on_type_id_and_index'
  end
end

class CreateStatuses < ActiveRecord::Migration[5.0]
  def change
    create_table :status_blocks do |t|
      t.integer :data_frame_id, index: true
      t.string  :data_frame_type, index: true
      t.integer :period_index, index: true
      t.integer :t1
      t.integer :t2
      t.integer :t3
      t.integer :t4
      t.integer :t5
      t.integer :t6
    end

    add_index :status_blocks, [:data_frame_id , :period_index, :data_frame_type], :unique => true, name: 'index_status_blocks_on_type_id_and_index'
  end
end


CreateAirports.migrate('up')
CreateTemperatures.migrate('up')
CreateDepartures.migrate('up')
CreateArrivals.migrate('up')
CreateStatuses.migrate('up')

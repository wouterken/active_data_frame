require 'test_helper'

class HasDataFrameTest < TransactionalTest

  def test_it_adds_a_class_level_data_frame_accessor
    frames = %i(arrivals departures temperature status)
    frames.each do |frame|
      assert(Airport.respond_to?(frame))
      assert_equal(Airport.send(frame).class, ActiveDataFrame::Table)
    end
  end

  def test_it_adds_an_instance_level_data_frame_accessor
    frames = %i(arrivals departures temperature status)
    airport = Airport.first
    frames.each do |frame|
      assert(airport.respond_to?(frame))
      assert_equal(airport.send(frame).class, ActiveDataFrame::Row)
    end
  end

  def test_it_supports_enum_value_mapping
    assert_equal Airport::Status, {normal: 0, alert: 1, critical: 2}
  end

  def test_it_supports_a_dot_accessible_hash_for_enum_value_mapping
    assert_equal Airport::Status.normal, 0
    assert_equal Airport::Status.alert, 1
    assert_equal Airport::Status.critical, 2
  end

  def test_it_implements_custom_inspect_functionality
    airport = Airport.include_temperature({'2001-01-01' => "temperature"})
                     .include_departures({'2001-01-01' => "departures"})
                     .first
    assert airport.inspect.include?('temperature')
    assert airport.inspect.include?('departures')
    refute airport.inspect.include?('arrivals')
  end

  def test_it_allows_definition_of_a_column_map
    assert Airport.df_column_maps['temperature'][Airport].empty?
    refute Airport.df_column_maps['status'][Airport].empty?
    assert Airport.df_column_maps['status'][Airport].keys.include?(:runways)
    assert_equal Airport.df_column_maps['status'][Airport].class, Hash
  end

  def test_it_allows_definition_of_a_column_names
    assert Airport.df_column_names['temperature'].nil?
    refute Airport.df_column_names['status'][Airport].empty?
    assert_equal Airport.df_column_names['status'][Airport].class, Array
    assert Airport.df_column_names['status'][Airport].include?(:runways)
  end

  def test_it_allows_definition_of_a_reverse_column_map
    my_map = {}
    Airport.status_reverse_column_map my_map
    assert_equal Airport.df_reverse_column_maps['status'][Airport].object_id, my_map.object_id
  end

  def test_it_defines_the_class_level_with_groups_method
    assert Airport.respond_to?(:with_groups)
    assert_equal Airport.with_groups(:country).class, ActiveDataFrame::GroupProxy
  end

  def test_it_supports_generating_subqueries_which_dynamically_include_columns

    # Show how we can select a single value from our data frame into our parent record
    assert_equal Airport.include_temperature('2001-01-01').first.attributes['2001-01-01'],
                 Airport.first.temperature['2001-01-01']

    # Show how we can select multiple values from our data frame into our parent record
    tp1 = "t#{(('2001-01-01'.to_time - Time.at(0)) / 1.hour).to_i}"
    tp2 = "t#{((('2001-01-01'.to_time + 30.minute) - Time.at(0)) / 1.hour).to_i}"
    with_days_departures = Airport.include_departures('2001-01-01'...'2001-02-01')

    assert with_days_departures.first.attributes.include?(tp1)
    assert with_days_departures.first.attributes.include?(tp2)
  end

  def test_it_supports_generating_subqueries_which_dynamically_includes_and_renames_columns

    assert_raises NoMethodError do
      Airport.first.temp
    end
    # Show how we can select a single value from our data frame into our parent record
    assert_equal Airport.include_temperature({'2001-01-01' => :temp}).first.temp,
                 Airport.first.temperature['2001-01-01']

    # Show how we can select multiple values from our data frame into our parent record
    with_days_departures = Airport.include_departures('2001-01-01'...'2001-01-02', as: :departures)
    assert with_days_departures.first.respond_to?(:departures1)
    assert with_days_departures.first.respond_to?(:departures24)
    refute with_days_departures.first.respond_to?(:departures25)

    assert_equal with_days_departures.last.departures['2001-01-01'], with_days_departures.last.departures1
    assert_equal with_days_departures.last.departures['2001-01-01 23:30'], with_days_departures.last.departures24
    # Show how we can select on these
    assert with_days_departures.where('departures1 + departures2 > 10').sum('(departures1 + departures2)')
  end

  def test_it_supports_combining_multiple_dynamically_included_columns_from_differing_tables
    with_temp_and_departure = Airport.include_temperature('2001-01-01'...'2001-01-02' , as: "temperature").include_departures('2001-01-01'...'2001-01-02' , as: "departures")
    assert with_temp_and_departure.sum('temperature10')
    assert with_temp_and_departure.sum('departures10')

    assert_equal with_temp_and_departure.first.temperature1, with_temp_and_departure.first.temperature['2001-01-01'].to_f
    assert_equal with_temp_and_departure.first.departures1, with_temp_and_departure.first.departures['2001-01-01'].to_f
  end

  def test_it_uses_plain_sql_for_good_cross_compatibility
    assert_equal Airport.include_temperature({'2001-01-01' => :t1}).to_sql,
                 Airport.from("(SELECT * FROM airports  LEFT JOIN(SELECT temperature_blocks.data_frame_type as btemperature_blocks11322_data_frame_type, temperature_blocks.data_frame_id btemperature_blocks11322_data_frame_id, temperature_blocks.t12 as \"t1\" FROM temperature_blocks  WHERE temperature_blocks.period_index = 11322) btemperature_blocks11322 ON btemperature_blocks11322.btemperature_blocks11322_data_frame_type = 'Airport' AND btemperature_blocks11322.btemperature_blocks11322_data_frame_id = airports.id) as airports").to_sql

    assert_equal Airport.include_temperature({'2001-01-01' => "custom_name", '3001-01-01' => "custom_name_2"}).to_sql,
                Airport.from("(SELECT * FROM airports  LEFT JOIN(SELECT temperature_blocks.data_frame_type as btemperature_blocks11322_data_frame_type, temperature_blocks.data_frame_id btemperature_blocks11322_data_frame_id, temperature_blocks.t12 as \"custom_name\" FROM temperature_blocks  WHERE temperature_blocks.period_index = 11322) btemperature_blocks11322 ON btemperature_blocks11322.btemperature_blocks11322_data_frame_type = 'Airport' AND btemperature_blocks11322.btemperature_blocks11322_data_frame_id = airports.id LEFT JOIN(SELECT temperature_blocks.data_frame_type as btemperature_blocks376564_data_frame_type, temperature_blocks.data_frame_id btemperature_blocks376564_data_frame_id, temperature_blocks.t12 as \"custom_name_2\" FROM temperature_blocks  WHERE temperature_blocks.period_index = 376564) btemperature_blocks376564 ON btemperature_blocks376564.btemperature_blocks376564_data_frame_type = 'Airport' AND btemperature_blocks376564.btemperature_blocks376564_data_frame_id = airports.id) as airports").to_sql

    assert_equal Airport.include_temperature({'2001-01-01' => "temperature"}).include_departures('2001-01-01' => "departures").to_sql,
                Airport.from("(SELECT * FROM airports  LEFT JOIN(SELECT temperature_blocks.data_frame_type as btemperature_blocks11322_data_frame_type, temperature_blocks.data_frame_id btemperature_blocks11322_data_frame_id, temperature_blocks.t12 as \"temperature\" FROM temperature_blocks  WHERE temperature_blocks.period_index = 11322) btemperature_blocks11322 ON btemperature_blocks11322.btemperature_blocks11322_data_frame_type = 'Airport' AND btemperature_blocks11322.btemperature_blocks11322_data_frame_id = airports.id LEFT JOIN(SELECT departure_blocks.data_frame_type as bdeparture_blocks11322_data_frame_type, departure_blocks.data_frame_id bdeparture_blocks11322_data_frame_id, departure_blocks.t12 as \"departures\" FROM departure_blocks  WHERE departure_blocks.period_index = 11322) bdeparture_blocks11322 ON bdeparture_blocks11322.bdeparture_blocks11322_data_frame_type = 'Airport' AND bdeparture_blocks11322.bdeparture_blocks11322_data_frame_id = airports.id) as airports").to_sql
  end
end
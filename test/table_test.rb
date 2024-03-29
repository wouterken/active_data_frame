require 'test_helper'

class TableTest < TransactionalTest
  def test_it_supports_setting_values
    nz_airports = Airport.where(country: 'NZ')
    us_airports = Airport.where(country: 'US')

    # Update
    nz_airports.departures['2001-01-01'] = [5,7,8]
    us_airports.departures['2001-01-01'] = [9,8,7]

    assert_equal nz_airports.departures.avg['2001-01-01'...'2001-01-01 03:00'], [5,7,8]
    assert_equal us_airports.departures.avg['2001-01-01'...'2001-01-01 03:00'], [9,8,7]

    # Create
    nz_airports.departures['3001-01-01'] = [5,7,8]
    us_airports.departures['3001-01-01'] = [9,8,7]

    assert_equal nz_airports.departures.avg['3001-01-01'...'3001-01-01 03:00'], [5,7,8]
    assert_equal us_airports.departures.avg['3001-01-01'...'3001-01-01 03:00'], [9,8,7]
  end

  def test_it_supports_getting_values
    assert_equal Airport.arrivals['2001-01-01'].length, Airport.count
    assert_equal Airport.arrivals['2001-01-01'..'2001-01-01 01:00'].length, Airport.count * 2
    assert_equal Airport.status.control_tower.to_a.uniq, [[:normal]]

    Airport.where(country: 'US').status[:control_tower] = [:critical]
    assert_equal Airport.status.control_tower.to_a.uniq.sort, [[:critical],[:normal]]
    assert_equal Airport.where(country: 'US').status.control_tower.to_a.uniq.sort, [[:critical]]
  end

  def test_it_supports_setting_hash_values
    random_airport_ids = Airport.pluck(:id).sample(12)
    random_airports = Airport.where(id: random_airport_ids)
    data = random_airport_ids.map do |id|
      [id, [*100...200].sample(10)]
    end.to_h
    departure_data = Airport.where(id: data.keys).departures['2001-01-01'...'2001-01-01 10:00']
    random_airports.each do |ap|
      refute_equal departure_data[ap], data[ap.id]
    end

    Airport.departures['2001-01-01'] = data
    departure_data = Airport.where(id: data.keys).departures['2001-01-01'...'2001-01-01 10:00']
    random_airports.each do |ap|
      assert_equal departure_data[ap], data[ap.id]
    end
  end

  def test_it_supports_row_mappings
    range = '2001-01-01'...'2001-01-01 10:00'
    arrivals_matrix = Airport.arrivals[range]
    # Row mapped matrix, is equal to individually retrieved rows
    Airport.find_each do |ap|
      assert_equal ap.arrivals[range], arrivals_matrix[ap]
    end
    random_data = Airport.pluck(:id).map do |id|
      [id, 10.times.map{ [:critical, :normal].sample }]
    end.to_h
    Airport.status[0] = random_data
    # Same goes for statuses
    status_matrix = Airport.status[0...10]
    Airport.find_each do |ap|
      assert_equal ap.status[0...10], status_matrix[ap]
      assert_equal ap.status[0...10].to_a, random_data[ap.id]
    end
  end

  def test_it_supports_deleting_values
    blocks_before = Blocks::ArrivalBlock.count

    date = '3001-01-01'
    Airport.arrivals[date] = [1100,1300, 900]
    # We've added new blocks
    assert blocks_before < Blocks::ArrivalBlock.count
    assert_equal Airport.arrivals.avg[date...'3001-01-01 03:00'], [1100, 1300, 900]
    assert_equal Airport.arrivals.sum[date...'3001-01-01 03:00'], [1100, 1300, 900].map{|x| x * Airport.count}

    # We've removed the new blocks
    Airport.arrivals.clear(date...'3001-01-01 03:00')
    assert_equal Airport.arrivals.max[date...'3001-01-01 03:00'], [0, 0, 0]
    assert_equal Airport.arrivals.min[date...'3001-01-01 03:00'], [0, 0, 0]
    assert_equal blocks_before, Blocks::ArrivalBlock.count
  end

  def test_it_supports_getting_nonexistent_values
    non_existent_date = '3001-01-01'
    assert_equal Airport.status[5000].to_a, [[:normal]] * Airport.count # Normal == 0
    assert_equal Airport.temperature[non_existent_date].T, [0] * Airport.count
  end

  def test_it_supports_avg_aggregate
    nz_airports = Airport.where(country: 'NZ')
    us_airports = Airport.where(country: 'US')
    # Single values
    assert_equal nz_airports.temperature.avg['2001-01-01'].round(2), (nz_airports.map{|a| a.temperature['2001-01-01'] }.sum(0) / nz_airports.count).round(2)
    assert_equal us_airports.temperature.avg['2001-01-01'].round(2), (us_airports.map{|a| a.temperature['2001-01-01'] }.sum(0) / us_airports.count).round(2)

    # Multiple values
    assert_equal nz_airports.temperature.avg['2001-01-01'..'2001-02-03'].round(2), (nz_airports.map{|a| a.temperature['2001-01-01'..'2001-02-03'] }.sum(0) / nz_airports.count).round(2)
    assert_equal us_airports.temperature.avg['2001-01-01'..'2001-02-03'].round(2), (us_airports.map{|a| a.temperature['2001-01-01'..'2001-02-03'] }.sum(0) / us_airports.count).round(2)
  end

  def test_it_supports_sum_aggregate
    # Single values
    assert_equal Airport.where(country: 'NZ').temperature.sum['2001-01-01'].round(2).to_f, Airport.where(country: 'NZ').map{|a| a.temperature['2001-01-01'].to_f }.sum.round(2)
    assert_equal Airport.where(country: 'US').temperature.sum['2001-01-01'].round(2).to_f, Airport.where(country: 'US').map{|a| a.temperature['2001-01-01'].to_f }.sum.round(2)

    # Multiple values
    assert_equal Airport.where(country: 'NZ').temperature.sum['2001-01-01'..'2001-02-03'].sum.round(2).to_f, Airport.where(country: 'NZ').map{|a| a.temperature['2001-01-01'..'2001-02-03'].sum }.sum.round(2).to_f
    assert_equal Airport.where(country: 'US').temperature.sum['2001-01-01'..'2001-02-03'].sum.round(2).to_f, Airport.where(country: 'US').map{|a| a.temperature['2001-01-01'..'2001-02-03'].sum }.sum.round(2).to_f
  end

  def test_it_supports_max_aggregate
    # Single values
    assert_equal Airport.where(country: 'NZ').temperature.max['2001-01-01'].round(2), Airport.where(country: 'NZ').map{|a| a.temperature['2001-01-01'].to_f }.max.round(2)
    assert_equal Airport.where(country: 'US').temperature.max['2001-01-01'].round(2), Airport.where(country: 'US').map{|a| a.temperature['2001-01-01'].to_f }.max.round(2)

    # Multiple values
    assert_equal Airport.where(country: 'NZ').temperature.max['2001-01-01'..'2001-02-03'].max.round(2), Airport.where(country: 'NZ').map{|a| a.temperature['2001-01-01'..'2001-02-03'].max.to_f }.max.round(2)
    assert_equal Airport.where(country: 'US').temperature.max['2001-01-01'..'2001-02-03'].max.round(2), Airport.where(country: 'US').map{|a| a.temperature['2001-01-01'..'2001-02-03'].max.to_f }.max.round(2)
  end

  def test_it_supports_min_aggregate
     # Single values
    assert_equal Airport.where(country: 'NZ').temperature.min['2001-01-01'].to_f.round(2), Airport.where(country: 'NZ').map{|a| a.temperature['2001-01-01'].to_f }.min.round(2)
    assert_equal Airport.where(country: 'US').temperature.min['2001-01-01'].to_f.round(2), Airport.where(country: 'US').map{|a| a.temperature['2001-01-01'].to_f }.min.round(2)

    # Multiple values
    assert_equal Airport.where(country: 'NZ').temperature.min['2001-01-01'..'2001-02-03'].min.round(2), Airport.where(country: 'NZ').map{|a| a.temperature['2001-01-01'..'2001-02-03'].min.to_f }.min.round(2)
    assert_equal Airport.where(country: 'US').temperature.min['2001-01-01'..'2001-02-03'].min.round(2), Airport.where(country: 'US').map{|a| a.temperature['2001-01-01'..'2001-02-03'].min.to_f }.min.round(2)
  end

  def test_idx_where_sum_gte
    summed_temps = Airport.temperature.sum['2001-01-01'...'2002-01-01'].sort
    third_largest = summed_temps[-4]
    assert_equal Airport.temperature.idx_where_sum_gte('2001-01-01'...'2002-01-01', third_largest).length, 3

    summed_temps = Airport.temperature.sum['2001-01-01'...'2002-01-01'].sort
    tenth_largest = summed_temps[-11]
    assert_equal Airport.temperature.idx_where_sum_gte('2001-01-01'...'2002-01-01', tenth_largest).length, 10
  end

  def test_idx_where_sum_lte
    summed_temps = Airport.temperature.sum['2001-01-01'...'2002-01-01'].sort
    third_smallest = summed_temps[3]
    assert_equal Airport.temperature.idx_where_sum_lte('2001-01-01'...'2002-01-01', third_smallest).length, 3

    summed_temps = Airport.temperature.sum['2001-01-01'...'2002-01-01'].sort
    tenth_smallest = summed_temps[10]
    assert_equal Airport.temperature.idx_where_sum_lte('2001-01-01'...'2002-01-01', tenth_smallest).length, 10
  end

  def test_it_doesnt_forward_scope_to_joined_tables
    # We want to ensure that the limit (20) applies to the airports,
    # and not to the total number of joined rows of airport and block rows.
    # If as expected we should see a large sum (> 2 million) if the limit
    # is applied to the joined rows this test will fail
    assert Airport.limit(20).temperature['2001-01-01'...'2002-01-01'].sum > 2_000_000
  end

end
require 'test_helper'

class DatabaseTest < TransactionalTest
  def test_it_supports_batching
    date    = '2001-01-01'
    date_to = '2001-01-01 03:00:00'
    count = Airport.count

    arrival_data = [60,55,50]
    departure_data = [60,55,50]

    ActiveDataFrame::Database.batch do
      Airport.departures[date] = departure_data
      Airport.arrivals[date]   = arrival_data
      # Not yet comitted
      refute_equal Airport.arrivals[date...date_to], count.times.map{ arrival_data }
      # Not yet comitted
      refute_equal Airport.departures[date...date_to], count.times.map{ departure_data }
    end
    # Comitted
    assert_equal Airport.arrivals[date...date_to], count.times.map{ arrival_data }
    # Comitted
    assert_equal Airport.departures[date...date_to], count.times.map{ departure_data }
  end

  def test_it_supports_bulk_insert
    count          = Airport.count
    future_date    = '3001-01-01'
    future_date_to = '3001-01-01 03:00:00'
    departures = [1,2,3]
    result = count.times.map{ departures }
    refute_equal Airport.departures[future_date...future_date_to], result
    Airport.departures[future_date] = departures
    assert_equal Airport.departures[future_date...future_date_to], result
  end

  def test_it_supports_bulk_update
    count = Airport.count
    date    = '2001-01-01'
    date_to = '2001-01-01 03:00:00'
    departures = [1,2,3]
    result = count.times.map{ departures }
    refute_equal Airport.departures[date...date_to], result
    Airport.departures[date] = departures
    assert_equal Airport.departures[date...date_to], result
  end

  def test_it_supports_bulk_delete
    date    = '2001-01-01'
    airport = Airport.first
    Blocks::ArrivalBlock.where(data_frame_id: Airport.first.id).delete_all

    arrival_data = (M.blank(columns: 10_000).random! * 100).to_type(M::Typecode::INT)
    airport.arrivals[date] = arrival_data
    refute_equal Blocks::ArrivalBlock.where(data_frame_id: Airport.first.id).count, 0

    airport.arrivals.clear(date.to_time...(date.to_time + 10_000.day))
    assert_equal Blocks::ArrivalBlock.where(data_frame_id: Airport.first.id).count, 0
  end
end
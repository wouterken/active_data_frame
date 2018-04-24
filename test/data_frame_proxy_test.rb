require 'test_helper'

class DataFrameProxyTest < TransactionalTest
  def test_it_exposes_a_value_map
    assert_equal Airport.status.value_map,
                 {:normal=>0, :alert=>1, :critical=>2}
    assert_equal Airport.status.value_map,
                 Airport.status.reverse_value_map.invert
  end

  def test_it_exposes_a_reverse_value_map
    assert_equal Airport.status.reverse_value_map,
                 {0=>:normal, 1=>:alert, 2=>:critical}
    assert_equal Airport.status.reverse_value_map,
                 Airport.status.value_map.invert
  end

  def test_it_supports_square_bracket_readers
    ap_singleton = Airport.first.status
    called = false
    ap_singleton.define_singleton_method(:get){|*args|
      called = true
      []
    }
    ap_singleton[1..10]
    assert called
  end

  def test_it_supports_square_bracket_writers
    ap_singleton = Airport.first.status
    called = false
    ap_singleton.define_singleton_method(:set){|*args|
      called = true
    }
    ap_singleton[1]= [1,2,3]
    assert called
  end

  def test_it_exposes_a_column_map
    assert_equal Airport.status.column_map,
                 {:runways=>0, :checkins=>1, :control_tower=>2, :weather=>3, :schedule=>4, :maintenance=>5}
  end

  def test_it_exposes_a_column_name_map
    assert_equal Airport.status.column_name_map,
                 [:runways, :checkins, :control_tower, :weather, :schedule, :maintenance]
  end

  def test_it_exposes_a_reverse_column_map
    assert_equal Airport.temperature.reverse_column_map.class,
                 Hash
  end

  def test_it_exposes_the_database_config
    assert_equal Airport.temperature.database.class, ActiveDataFrame::Database
  end

  def test_it_exposes_named_columns_through_method_missing
    Airport.status.column_name_map.each do |column|
      assert_equal Airport.first.status.send(column), [[:normal]]
    end
  end

  def test_unmap_ranges
    proxy = Airport.temperature
    # Does nothing on empty ranges
    assert_equal proxy.unmap_ranges([], {}), []

    # Does nothing on ranges that are already int..int
    assert_equal proxy.unmap_ranges([1..10], {}), [1..10]

    # Can handle multiple ranges
    assert_equal proxy.unmap_ranges([1..10, 5..20], {}), [1..10, 5..20]

    # Doesn't mutate ranges that are not in the map
    assert_equal proxy.unmap_ranges(['no'..'yes'], {}), ["no".."yes"]

    # Doesn't allow mapping ranges of non uniform types
    assert_raises ArgumentError do
      proxy.unmap_ranges(['no'..'yes'], {no: 0, "yes" => 4})
    end

    # Will map ranges of any type to integer ranges if both exist in the map
    assert_equal proxy.unmap_ranges(['no'..'yes'], {"no" => 0, "yes" => 4}), [0..4]
  end

  def test_get_bounds
    # Spans multiple
    proxy = Airport::arrivals
    range,* = proxy.unmap_ranges(['2001-01-01'...'2001-02-01'], Airport::arrivals.column_map)
    bounds  = proxy.get_bounds(range.first, range.last)

    assert_equal bounds.from.index, range.first / proxy.block_type::BLOCK_SIZE
    assert_equal bounds.from.offset, range.first % proxy.block_type::BLOCK_SIZE

    assert_equal bounds.to.index, range.last / proxy.block_type::BLOCK_SIZE
    assert_equal bounds.to.offset, range.last % proxy.block_type::BLOCK_SIZE

    # Span of 1
    proxy = Airport::departures
    range,* = proxy.unmap_ranges(['2001-01-01'...'2001-01-01'], Airport::departures.column_map)
    bounds  = proxy.get_bounds(range.first, range.last)

    assert_equal bounds.from.index, range.first / proxy.block_type::BLOCK_SIZE
    assert_equal bounds.from.offset, range.first % proxy.block_type::BLOCK_SIZE

    assert_equal bounds.to.index, range.last / proxy.block_type::BLOCK_SIZE
    assert_equal bounds.to.offset, range.last % proxy.block_type::BLOCK_SIZE

    # Span with no mapped columns
    proxy   = Airport::status
    range,* = proxy.unmap_ranges([0..10], Airport::status.column_map)
    bounds  = proxy.get_bounds(range.first, range.last)

    assert_equal bounds.from.index, range.first / proxy.block_type::BLOCK_SIZE
    assert_equal bounds.from.offset, range.first % proxy.block_type::BLOCK_SIZE

    assert_equal bounds.to.index, range.last / proxy.block_type::BLOCK_SIZE
    assert_equal bounds.to.offset, range.last % proxy.block_type::BLOCK_SIZE
  end

  def test_iterate_bounds
    proxy = Airport::temperature
    bounds = proxy.unmap_ranges(
      ['2001-01-01'...'2001-02-01','2001-02-01'...'2001-03-01'],
      Airport::departures.column_map
    ).map do |range|
      proxy.get_bounds(range.first, range.last)
    end
    collected = []
    proxy.iterate_bounds(bounds) do |index, left, right, cursor, size|
      collected << [index, left, right, cursor, size]
    end

    block_count_b1 = bounds[0].to.index - bounds[0].from.index
    block_count_b2 = bounds[1].to.index - bounds[1].from.index

    assert_equal collected.length, block_count_b1.succ + block_count_b2.succ
    assert_equal collected.map(&:first)[0..block_count_b1], [*bounds[0].from.index..bounds[0].to.index]
    assert_equal collected.map(&:first)[block_count_b1.succ..-1], [*bounds[1].from.index..bounds[1].to.index]

    # 24 is hours in a day, block_count_b1 is number of days
    assert_equal collected.map(&:last)[0..block_count_b1].sum.-(1) / 24.0, block_count_b1
    assert_equal collected.map(&:last)[block_count_b1.succ..-1].sum.-(1) / 24.0, block_count_b2
  end

  def test_match_range
    # Simplify ranges of length 1 for faster queries
    assert_equal Airport::arrivals.match_range(1,1), 1
    assert_equal Airport::arrivals.match_range(8,8), 8

    # Other ranges are untouched (and inclusive)
    assert_equal Airport::arrivals.match_range(1,10), 1..10
    assert_equal Airport::arrivals.match_range(8,-1), 8..-1
  end

  def test_blocks_between
    proxy = Airport::arrivals
    range,* = proxy.unmap_ranges(['2001-01-01'...'2001-02-01'], Airport::arrivals.column_map)
    bounds  = proxy.get_bounds(range.first, range.last)
    blocks  = proxy.blocks_between([bounds])
    assert_equal blocks.find_each.map(&:period_index).uniq.sort, [*range.first / proxy.block_type::BLOCK_SIZE..range.last / proxy.block_type::BLOCK_SIZE]
  end
end
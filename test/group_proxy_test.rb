require 'test_helper'

class GroupProxyTest < TransactionalTest
  def test_support_for_grouped_min
    date = '2001-01-01'
    group = Airport.include_temperature(date => :temperature).with_groups(:country)
    assert_equal group.min(:temperature).class, Hash
    assert_equal group.min(:temperature)['US'].round(2), Airport.where(country: :US).to_a.map{|a| a.temperature[date].to_f }.min.round(2)
  end

  def test_support_for_grouped_max
    date = '2001-01-01'
    group = Airport.include_temperature(date => :temperature).with_groups(:country)
    assert_equal group.max(:temperature).class, Hash
    assert_equal group.max(:temperature)['US'].round(2), Airport.where(country: :US).to_a.map{|a| a.temperature[date].to_f }.max.round(2)
  end

  def test_support_for_grouped_sum
    date = '2001-01-01'
    group = Airport.include_temperature(date => :temperature).with_groups(:country)
    assert_equal group.sum(:temperature).class, Hash
    assert_equal group.sum(:temperature)['US'].round(2), Airport.where(country: :US).to_a.sum{|a| a.temperature[date].to_f }.round(2)
  end

  def test_support_for_grouped_average
    date = '2001-01-01'
    group = Airport.include_temperature(date => :temperature).with_groups(:country)
    assert_equal group.average(:temperature).class, Hash
    assert_equal(
      group.average(:temperature)['US'].round(2),
      (Airport.where(country: :US).to_a.sum{|a| a.temperature[date].to_f } / Airport.where(country: :US).length).round(2)
    )
  end

  def test_support_for_grouped_count
    assert_equal Airport.with_groups(:country).count.class, Hash
    assert_equal Airport.with_groups(:country).count['US'], Airport.where(country: :US).length
  end

end
require 'csv'

class Airport < ActiveRecord::Base
  include HasTemperature::ColumnMaps, HasTemperature,
          HasArrivals::ColumnMaps,    HasArrivals,
          HasDepartures::ColumnMaps,  HasDepartures,
          HasStatus::ColumnMaps,      HasStatus
end

normal      = ->{ M.blank(columns: 365 * 24, rows: 3).random!.sum(1) }
departures  = ->{ (1000 * normal[]).to_type(3) }
arrivals    = ->{ (1000 * normal[]).to_type(3) }
temperature = ->{ (20   * normal[]) - 10 }
status      = ->{ M.blank(columns: 6).to_type(3)}

Airport.create!(
  CSV.foreach(File.expand_path('../codes.csv', __FILE__), headers: true).map(&:to_hash)
)

Airport.find_each do |a|
  ActiveDataFrame::Database.batch do
    a.departures['2001-01-01']  = departures[]
    a.arrivals['2001-01-01']    = arrivals[]
    a.temperature['2001-01-01'] = temperature[]
    a.status[0]                 = status[]
  end
end
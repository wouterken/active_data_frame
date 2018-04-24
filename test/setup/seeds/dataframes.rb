
module HasTemperature
  include ActiveDataFrame::HasDataFrame('temperature', Blocks::TemperatureBlock)
  module ColumnMaps
    def self.included(base)
      base.temperature_column_map Hash.new{|hash, time| ((time.to_time - Time.at(0)) / 1.hour).to_i}
    end
  end
end
module HasDepartures
  include ActiveDataFrame::HasDataFrame('departures', Blocks::DepartureBlock)
  module ColumnMaps
    def self.included(base)
      base.departures_column_map Hash.new{|hash, time| ((time.to_time - Time.at(0)) / 1.hour).to_i}
    end
  end
end
module HasArrivals
  include ActiveDataFrame::HasDataFrame('arrivals', Blocks::ArrivalBlock)
  module ColumnMaps
    def self.included(base)
      base.arrivals_column_map Hash.new{|hash, time| ((time.to_time - Time.at(0)) / 1.hour).to_i rescue time.to_i }
    end
  end
end
module HasStatus
  include ActiveDataFrame::HasDataFrame('status', Blocks::StatusBlock, value_map: {
    normal: 0,
    alert: 1,
    critical: 2
  })
  module ColumnMaps
    def self.included(base)
      base.status_column_names %i(runways checkins control_tower weather schedule maintenance)
    end
  end
end

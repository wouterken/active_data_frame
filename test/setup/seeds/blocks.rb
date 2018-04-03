module Blocks
  class TemperatureBlock < ActiveRecord::Base
    BLOCK_SIZE = 24
    COLUMNS    = %w(t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19 t20 t21 t22 t23 t24)
    TYPECODE   = 5
    self.table_name = 'temperature_blocks'
  end
  class ArrivalBlock < ActiveRecord::Base
    BLOCK_SIZE = 24
    COLUMNS    = %w(t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19 t20 t21 t22 t23 t24)
    TYPECODE   = 3
    self.table_name = 'arrival_blocks'
  end
  class DepartureBlock < ActiveRecord::Base
    BLOCK_SIZE = 24
    COLUMNS    = %w(t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19 t20 t21 t22 t23 t24)
    TYPECODE   = 3
    self.table_name = 'departure_blocks'
  end
  class StatusBlock < ActiveRecord::Base
    BLOCK_SIZE = 6
    COLUMNS    = %w(t1 t2 t3 t4 t5 t6)
    TYPECODE   = 3
    self.table_name = 'status_blocks'
  end
end

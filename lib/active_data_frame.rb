require 'active_data_frame/data_frame_proxy'
require 'active_data_frame/group_proxy'
require 'active_data_frame/table'
require 'active_data_frame/row'
require 'active_data_frame/has_data_frame'
require 'active_data_frame/database'
require 'rmatrix'

module ActiveDataFrame
  CONFIG = OpenStruct.new({
    suppress_logs: false,
    insert_max_batch_size: 10_000,
    update_max_batch_size: 10_000,
    delete_max_batch_size: 10_000,
  })

  module_function
    def config
      yield CONFIG
    end

    CONFIG.each_pair do |(key)|
      define_method(key){ CONFIG.send(key) }
    end
end
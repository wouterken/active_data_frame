module ActiveDataFrame
  class Bounds < Struct.new(:from, :to, :length, :index)
  end
end
module ActiveDataFrame
  class GroupProxy
    attr_accessor :groups
    def initialize(groups)
      self.groups = groups
    end

    def min(column_name)
      aggregate('minimum', column_name)
    end

    def max(column_name)
      aggregate('maximum', column_name)
    end

    def sum(column_name)
      aggregate('sum', column_name)
    end

    def average(column_name)
      aggregate('average', column_name)
    end

    def count
      aggregate('count')
    end

    private
      def aggregate *agg
        counts = self.groups.send(*agg)
        grouped = {}
        counts.each do |keys, value|
          keys = Array(keys)
          child = keys[0..-2].reduce(grouped){|parent, key| parent[key] ||= {}}
          child[keys[-1]] = value
        end
        grouped
      end
  end
end
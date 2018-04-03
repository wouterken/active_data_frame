
# Get times of day where there was a price spike in wellington
Icp.where(region: :wellington).loads.idx_where_sum_gte(Time.now..1.day.from_now, 12_000)

# Get current load for all Icps, grouped by :region, :customer_class, :tariff
Icp.include_loads(Time.now).with_groups(:region, :customer_class, :tariff).sum("\"#{Time.now}\"")

# Get next days aggregate usage for Auckland residential customers
Icp.where(region: :auckland, customer_class: :residential).loads.sum(Time.now..1.day.from_now)

# Get a years worth of load for a single ICP
Icp.first.load[Time.now..1.year.from_now]

# Get a days worth of load for many ICPs
Icp.where(tariff: :un).loads[Time.now..1.day.from_now]

# Get a average load over a day  load for many ICPs
Icp.where(tariff: :un).loads.avg(Time.now..1.day.from_now)

# Count icps which have more than 5.5kw of load at this point in time
Icp.include_loads(Time.now).where("\"%s\" > ?" % Time.now, 5.5).count


# See the largest spepal length seen for each speacies
Iris.with_groups(:species).max(:sepal_length)

# Get individual iris sepal_length
Iris.first.dimension.sepal_length

# Get multiple dimensions for individual iris
Iris.first.dimension[:sepal_length, :petal_width]

# Get range of dimensions for individual iris
Iris.first.dimension[:sepal_length..:petal_width]

# Get range of dimensions for all iris versicolors
dimensions = Iris.where(species: :versicolor).dimensions[:sepal_length..:petal_width]

# Chop data as needed
sepal_lengths = dimensions.sepal_length
sepal_lengths_petal_widths = dimensions[[:sepal_length, :petal_width]]

selected_iris = dimensions[Iris.where(species: :versicolor).first(5)]

# Look at RMatrix API for matrix functionality
#
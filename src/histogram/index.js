const cw = require('@aws-sdk/client-cloudwatch');

const DEFAULT_BUCKET_COUNT = 100;
const MIN_BUCKET_COUNT = 1;
const MAX_BUCKET_COUNT = 500;
const BASIC_STATS = ['Minimum', 'Maximum', 'SampleCount', 'Sum'];
const ZERO_VALUE = 0.0;
const EPSILON = 0.1;
const BIN_SIZE = Math.log(1 + EPSILON);
const MAX_BIN_RANGE = 7000;
const MIN_BIN_RANGE = -MAX_BIN_RANGE;
const ZERO_VALUE_BIN = MIN_BIN_RANGE - 1;
const NEGATIVE_ONE_BIN_OFFSET = -2 * MAX_BIN_RANGE - 2;
const SMALLEST_BIN = NEGATIVE_ONE_BIN_OFFSET + MAX_BIN_RANGE;
const MIN_VALUE_FOR_HIST = 0.0001;

const describeGetMetricDataEventHandler = () => {
    const FIRST_EXAMPLE_DEFAULT_ARGS_LIST = ['AWS/Lambda, Duration', 100];
    const FIRST_EXAMPLE_DEFAULT_ARGS_STRING = FIRST_EXAMPLE_DEFAULT_ARGS_LIST.map((arg) =>
        typeof arg === 'string' ? `'${arg}'` : arg
    ).join(', ');
    const DESCRIPTION = `
## Sample Cloudwatch histogram plotter

Plots a logarithmic distribution of measurements for any CloudWatch metric that supports [percentiles](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Percentiles).

* To be able to see the graph, select *Bar* chart visualization in *Graph options*
* Height of each bar is the number of samples in each bucket
* Label of each bar is "center" value of bucket
* Moving the graph legend to the right can help with visualizing

### Query arguments

\\# | Type | Description
---|---|---
1 | String | The full name of the metric, in format <Namespace>, <MetricName>, <Dim Name 1>, <Dim Value 1>,... etc. URL encode the strings between commas.
2 | Number | (optional) max number of buckets, number from ${MIN_BUCKET_COUNT} to ${MAX_BUCKET_COUNT} (defaults to ${DEFAULT_BUCKET_COUNT})

### Example Expression

Plot the histogram of all Lambda function calls:

\`\`\`
LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', ${FIRST_EXAMPLE_DEFAULT_ARGS_STRING})
\`\`\`
    `;

    return {
        DataSourceConnectorName: process.env.AWS_LAMBDA_FUNCTION_NAME,
        ArgumentDefaults: FIRST_EXAMPLE_DEFAULT_ARGS_LIST.map((arg) => {
            return { Value: arg };
        }),
        Description: DESCRIPTION,
    };
};

const constrainBinToAllowedRange = (binNumber) => {
    if (binNumber > MAX_BIN_RANGE) {
        return MAX_BIN_RANGE;
    }
    if (binNumber < MIN_BIN_RANGE) {
        return MIN_BIN_RANGE;
    }
    return binNumber;
};

const getBinNumber = (value) => {
    if (value === ZERO_VALUE) {
        return ZERO_VALUE_BIN;
    }
    let binNumber = Math.floor(Math.log(Math.abs(value)) / BIN_SIZE);
    binNumber = constrainBinToAllowedRange(binNumber);
    return value > ZERO_VALUE ? binNumber : NEGATIVE_ONE_BIN_OFFSET - binNumber;
};

const getValueWithinBin = (binNum, offset) => {
    let binNumber = binNum;
    if (binNumber === ZERO_VALUE_BIN) {
        return ZERO_VALUE;
    }
    let sign = 1;
    if (binNumber <= SMALLEST_BIN) {
        binNumber = NEGATIVE_ONE_BIN_OFFSET - binNumber;
        sign = -1;
    }
    binNumber = constrainBinToAllowedRange(binNumber);
    return sign * Math.exp((binNumber + offset) * BIN_SIZE);
};

const getBinTop = (binNumber) => {
    return getValueWithinBin(binNumber + 1, 0);
};

const getLabelForValue = (value, unit) => {
    let label;
    const isMillis = /millis/i.test(unit);
    const isSeconds = /^sec/i.test(unit);

    if (isMillis) {
        if (value < 1000) {
            label = `${value.toPrecision(3)}ms`;
        } else if (value < 1000000) {
            label = `${(value / 1000).toPrecision(3)}s`;
        } else {
            label = `${(value / 1000).toPrecision(6)}s`;
        }
    } else if (isSeconds) {
        if (value < 1) {
            label = `${(value * 1000).toPrecision(3)}ms`;
        } else if (value < 1000) {
            label = `${value.toPrecision(3)}s`;
        } else {
            label = `${value.toPrecision(6)}s`;
        }
    } else {
        label = `${value.toPrecision(6)}`;
    }

    return label;
};

const parseFullMetric = (fullMetricString) => {
    const metricFields = fullMetricString.split(',').map((field) => field.trim());
    if (metricFields.length < 2 || metricFields.length % 2 !== 0) {
        throw Error(
            `Malformed full metric name, expected <Namespace>,<MetricName>,<DimPair Name 1>,<DimPair Value 1>,... etc`,
            { cause: 'Validation' }
        );
    }
    const Namespace = decodeURIComponent(metricFields[0]);
    const MetricName = decodeURIComponent(metricFields[1]);
    const Dimensions = [];
    for (let dimIndex = 2; dimIndex < metricFields.length - 1; dimIndex += 2) {
        Dimensions.push({
            Name: decodeURIComponent(metricFields[dimIndex]),
            Value: decodeURIComponent(metricFields[dimIndex + 1]),
        });
    }
    return { Namespace, MetricName, Dimensions };
};

const validateGetMetricDataRequest = (Arguments) => {
    if (Arguments.length !== 1 && Arguments.length !== 2) {
        throw Error(`Expected 1 or 2 arguments, received ${Arguments.length}`, { cause: 'Validation' });
    }
    const fullMetric = Arguments[0];
    let bucketCount = DEFAULT_BUCKET_COUNT;
    if (Arguments.length === 2) {
        bucketCount = parseInt(Arguments[1], 10);
    }
    if (typeof fullMetric !== 'string') {
        throw Error(`Unexpected argument type, expected (<string>[, <number>])`, { cause: 'Validation' });
    }
    const metric = parseFullMetric(fullMetric);

    if (bucketCount < MIN_BUCKET_COUNT || bucketCount > MAX_BUCKET_COUNT) {
        throw Error(
            `Bucket count (${bucketCount}) outside of valid range (${MIN_BUCKET_COUNT} to ${MAX_BUCKET_COUNT})`,
            { cause: 'Validation' }
        );
    }

    return { metric, bucketCount };
};

const getMetricBasicStats = async (metric, StartTime, EndTime, rangePeriod, region) => {
    const metricQueries = BASIC_STATS.map((stat) => {
        return {
            Id: `m${stat}`,
            MetricStat: {
                Metric: metric,
                Stat: stat,
                Period: rangePeriod,
            },
        };
    });
    const getMetricDataParams = {
        MetricDataQueries: metricQueries,
        StartTime: new Date(StartTime * 1000),
        EndTime: new Date(EndTime * 1000),
    };
    const cloudwatch = new cw.CloudWatch({ region });
    const getMetricDataResult = await cloudwatch.getMetricData(getMetricDataParams);

    const basicStats = {};
    getMetricDataResult.MetricDataResults.forEach((metricData) => {
        const stat = metricData.Id.replace(/^./, ''); // Drop 'm' at start of Id to restore the stat name
        const value = metricData.Values.shift();
        basicStats[stat] = value;
    });
    return basicStats;
};

const getHistogramMetricDefinitions = (metric, min, max, rangePeriod, bucketCount) => {
    const metricDefinitions = [];
    const useZeroBucketForMin = !!(min < MIN_VALUE_FOR_HIST);
    const minBucketNum = useZeroBucketForMin ? getBinNumber(MIN_VALUE_FOR_HIST) : getBinNumber(min);
    const maxBucketNum = getBinNumber(max);
    const numBuckets = maxBucketNum - minBucketNum;
    const incBucket = numBuckets / bucketCount;
    let currentBucketBottom = min;

    for (let bin = minBucketNum; bin <= maxBucketNum; bin += incBucket) {
        const binRound = Math.round(bin);
        const top = getBinTop(binRound);
        const middle = (top + currentBucketBottom) / 2;

        // Use PR stats to get percentage of samples in each histogram bucket
        const stat = `PR(${currentBucketBottom.toPrecision(6)}:${top.toPrecision(6)})`;
        const label = getLabelForValue(middle);

        metricDefinitions.push({
            Id: `m${binRound}`,
            Label: label,
            MetricStat: {
                Metric: metric,
                Stat: stat,
                Period: rangePeriod,
            },
        });

        currentBucketBottom = top;
    }

    return metricDefinitions;
};

const getHistogramData = async (metric, StartTime, EndTime, rangePeriod, region, bucketCount, basicStats) => {
    const metricQueries = getHistogramMetricDefinitions(
        metric,
        basicStats.Minimum,
        basicStats.Maximum,
        rangePeriod,
        bucketCount
    );
    const getMetricDataParams = {
        MetricDataQueries: metricQueries,
        StartTime: new Date(StartTime * 1000),
        EndTime: new Date(EndTime * 1000),
    };
    const cloudwatch = new cw.CloudWatch({ region });
    const getMetricDataResult = await cloudwatch.getMetricData(getMetricDataParams);
    const metricData = getMetricDataResult.MetricDataResults;

    // Convert timestamps from Date() -> seconds, which is what is expected format from Lambda
    // Convert percentage values from PR stats to Counts
    metricData.forEach((metric) => {
        /* eslint no-param-reassign: ["error", { "props": false }] */
        metric.Timestamps = metric.Timestamps.map((ts) => {
            return ts.getTime() / 1000;
        });
        metric.Values = metric.Values.map((percent) => {
            return Math.round((percent * basicStats.SampleCount) / 100);
        });
        metric.Unit = 'Count';
    });
    return { MetricDataResults: metricData };
};

const getMetricDataEventHandler = async (event) => {
    const { StartTime, EndTime, Period, Arguments } = event.GetMetricDataRequest;
    const { region } = event;
    const { metric, bucketCount } = validateGetMetricDataRequest(Arguments);

    // Calculate a valid period matching the full time range, rounded up to Period
    const timeRange = EndTime - StartTime;
    const rangePeriod = timeRange - (timeRange % Period) + Period;

    // Get Minimum and Maximum of metric first, to determine limits of "buckets"
    const basicStats = await getMetricBasicStats(metric, StartTime, EndTime, rangePeriod, region);

    // Now get all the buckets, knowing the limits of data
    const histogramData = await getHistogramData(
        metric,
        StartTime,
        EndTime,
        rangePeriod,
        region,
        bucketCount,
        basicStats
    );

    return histogramData;
};

exports.handler = async (event) => {
    try {
        switch (event.EventType) {
            case 'GetMetricData':
                return await getMetricDataEventHandler(event);
            case 'DescribeGetMetricData':
                return describeGetMetricDataEventHandler(event);
            default:
                throw Error(`Unknown EventType: ${event.EventType}`, { cause: 'Validation' });
        }
    } catch (err) {
        const message = err.message || err;
        const code = err.cause || 'InternalError';
        return { Error: { Code: code, Value: message } };
    }
};

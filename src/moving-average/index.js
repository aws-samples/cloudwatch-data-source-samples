const cw = require('@aws-sdk/client-cloudwatch');

const describeGetMetricDataEventHandler = () => {
    const FIRST_EXAMPLE_DEFAULT_ARGS_LIST = ['AWS/Lambda,Duration', 'Average', 10];
    const FIRST_EXAMPLE_DEFAULT_ARGS_STRING = FIRST_EXAMPLE_DEFAULT_ARGS_LIST.map((arg) =>
        typeof arg === 'string' ? `'${arg}'` : arg
    ).join(', ');
    const DESCRIPTION = `
## Sample Cloudwatch Metric moving average data source connector

Returns the moving average for a CloudWatch Metric. Each datapoint is the average of the original datapoint and the trailing N - 1 datapoints. Missing data is ignored.

### Query arguments

\\# | Type | Description
---|---|---
1 | String | The full name of the metric, in format <Namespace>,<MetricName>,<Dim Name 1>,<Dim Value 1>,... etc. URL encode the strings between commas
2 | String | The statistic to retrieve for the metric
3 | Number | The number of datapoints to average, from 2 upwards

### Example Expression
Plot 10-datapoint moving average of duration of all Lambda functions:

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
    if (Arguments.length !== 3) {
        throw Error(`Expected 3 arguments, received ${Arguments.length}`, { cause: 'Validation' });
    }
    const [fullMetric, stat, numberOfDatapoints] = Arguments;

    if (typeof fullMetric !== 'string' || typeof stat !== 'string' || typeof numberOfDatapoints !== 'number') {
        throw Error(`Unexpected argument type, expected (<string>, <string>, <number>)`, { cause: 'Validation' });
    }
    if (numberOfDatapoints < 2) {
        throw Error(`Number of datapoints, ${numberOfDatapoints}, must be greater than 1`, {
            cause: 'Validation',
        });
    }

    const metric = parseFullMetric(fullMetric);

    return { metric, stat, numberOfDatapoints };
};

const metricDataToMovingAverage = (metricData, revisedStartTime, StartTime, EndTime, Period) => {
    // Build map of timestamp -> value
    const timestampMap = {};
    metricData.Timestamps.forEach((ts, index) => {
        timestampMap[ts.getTime() / 1000] = metricData.Values[index];
    });

    const averagedMetricData = {
        Timestamps: [],
        Values: [],
        Label: metricData.Label,
        Status: 'Complete',
    };

    // Let's get first averaged datapoint, and use that along the time-series to calculate the rest
    const windowOfValues = [];
    let currentSampleCount = 0;
    let sampleTotal = 0;
    for (let time = revisedStartTime; time < EndTime; time += Period) {
        const value = timestampMap[time];
        windowOfValues.push(value);
        if (value !== undefined) {
            currentSampleCount++;
            sampleTotal += value;
        }
        if (time > StartTime) {
            const firstWindowValue = windowOfValues.shift();
            if (firstWindowValue !== undefined) {
                currentSampleCount--;
                sampleTotal -= firstWindowValue;
            }
        }
        if (time >= StartTime) {
            if (currentSampleCount > 0) {
                // We have a valid average
                averagedMetricData.Timestamps.push(time);
                averagedMetricData.Values.push(sampleTotal / currentSampleCount);
            }
        }
    }

    return averagedMetricData;
};

const getMetricDataEventHandler = async (event) => {
    const { StartTime, EndTime, Period, Arguments } = event.GetMetricDataRequest;
    const { metric, stat, numberOfDatapoints } = validateGetMetricDataRequest(Arguments);

    // Go back "n - 1" periods of time, to retrieve enough data to calculate moving average for all data in StartTime -> EndTime
    const revisedStartTime = StartTime - (numberOfDatapoints - 1) * Period;
    const getMetricDataParams = {
        MetricDataQueries: [
            {
                Id: 'm1',
                MetricStat: {
                    Metric: metric,
                    Stat: stat,
                    Period,
                },
            },
            {
                Id: 'timer',
                Expression: 'TIME_SERIES(10)',
                Period,
            },
        ],
        StartTime: new Date(revisedStartTime * 1000),
        EndTime: new Date(EndTime * 1000),
    };
    const { region } = event;
    const cloudwatch = new cw.CloudWatch({ region });
    const getMetricDataResult = await cloudwatch.getMetricData(getMetricDataParams);
    const firstMetric = getMetricDataResult.MetricDataResults[0];
    const secondMetric = getMetricDataResult.MetricDataResults[1];
    const metricData = firstMetric.Id === 'm1' ? firstMetric : secondMetric;
    const timerData = secondMetric.Id === 'timer' ? secondMetric : firstMetric;
    const earliestTimestamp = timerData.Timestamps.reduce((acc, val) => (acc < val ? acc : val));
    const averagedMetricData = metricDataToMovingAverage(
        metricData,
        earliestTimestamp ? earliestTimestamp.getTime() / 1000 : revisedStartTime,
        StartTime,
        EndTime,
        Period,
        numberOfDatapoints
    );

    return { MetricDataResults: [averagedMetricData] };
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

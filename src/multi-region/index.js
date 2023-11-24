const cw = require('@aws-sdk/client-cloudwatch');

const describeGetMetricDataEventHandler = () => {
    const FIRST_EXAMPLE_DEFAULT_ARGS_LIST = [
        'AWS/Usage, CallCount, Type, API, Resource, GetMetricData, Service, CloudWatch, Class, None',
        'Sum',
        'us-east-1, eu-west-1',
    ];
    const FIRST_EXAMPLE_DEFAULT_ARGS_STRING = FIRST_EXAMPLE_DEFAULT_ARGS_LIST.map((arg) =>
        typeof arg === 'string' ? `'${arg}'` : arg
    ).join(', ');
    const DESCRIPTION = `
## Sample Cloudwatch multi-region data source connector

Loads a CloudWatch metric from one or more regions. This enables:
* Alarming on a metric in a different region
* Alarming on combination of metrics from multiple regions

### Query arguments

\\# | Type | Description
---|---|---
1 | String | The full name of the metric, in format <Namespace>, <MetricName>, <Dim Name 1>, <Dim Value 1>,... etc. URL encode the strings between commas.
2 | String | The [CloudWatch statistic](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Statistics-definitions.html) to retrieve for the metric
3 | String | Comma-separated lists of regions to load metric from, e.g. \`us-east-1, eu-west-1\`

### Example Expression

Display the number of calls to CloudWatch GetMetricData in us-east-1 and eu-west-1.

\`\`\`
LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', ${FIRST_EXAMPLE_DEFAULT_ARGS_STRING})
\`\`\`

Sum the total calls to CloudWatch GetMetricData in us-east-1 and eu-west-1, ready for alarming on:

\`\`\`
SUM(LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', 'AWS/Usage, CallCount, Type, API, Resource, GetMetricData, Service, CloudWatch, Class, None', 'Sum', 'us-east-1, eu-west-1'))
\`\`\`

Display the average EC2 CPU usage across US regions.

\`\`\`
LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', 'AWS/EC2, CPUUtilization', 'Average', 'us-east-1, us-east-2, us-west-1, us-west-2')
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
    const [fullMetric, stat, regions] = Arguments;

    if (typeof fullMetric !== 'string' || typeof stat !== 'string' || typeof regions !== 'string') {
        throw Error(`Unexpected argument type, expected (<string>, <string>, <string>)`, { cause: 'Validation' });
    }
    const metric = parseFullMetric(fullMetric);
    const regionList = regions
        .split(',')
        .map((region) => region.trim())
        .filter((reg) => reg);
    if (regionList.length < 1) {
        throw Error(`Expected at least one region`, { cause: 'Validation' });
    }

    return { metric, stat, regionList };
};

const getMetricDataEventHandler = async (event) => {
    const { StartTime, EndTime, Period, Arguments } = event.GetMetricDataRequest;
    const { metric, stat, regionList } = validateGetMetricDataRequest(Arguments);
    const metricStat = {
        Metric: metric,
        Stat: stat,
        Period,
    };
    const getMetricDataCalls = regionList.map((region) => {
        const getMetricDataParams = {
            MetricDataQueries: [{ Id: region.replace(/-/g, '_'), MetricStat: metricStat }],
            StartTime: new Date(StartTime * 1000),
            EndTime: new Date(EndTime * 1000),
        };
        const cloudwatch = new cw.CloudWatch({ region });
        return cloudwatch.getMetricData(getMetricDataParams);
    });

    const getMetricDataResults = await Promise.all(getMetricDataCalls);
    const collatedResults = getMetricDataResults.map((getMetricDataResult) => {
        const metric = getMetricDataResult.MetricDataResults[0];
        metric.Label = metric.Id.replace(/_/g, '-');
        metric.Timestamps = metric.Timestamps.map((ts) => {
            return ts.getTime() / 1000;
        });
        delete metric.Id;
        metric.StatusCode = 'Complete';
        return metric;
    });

    return { MetricDataResults: collatedResults };
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

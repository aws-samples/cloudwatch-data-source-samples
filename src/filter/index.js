const cw = require('@aws-sdk/client-cloudwatch');

const describeGetMetricDataEventHandler = () => {
    const FIRST_EXAMPLE_DEFAULT_ARGS_LIST = [
        'SEARCH("{AWS/EC2,InstanceId} MetricName=CPUUtilization", "Average")',
        'MAX > 70',
    ];
    const FIRST_EXAMPLE_DEFAULT_ARGS_STRING = FIRST_EXAMPLE_DEFAULT_ARGS_LIST.map((arg) =>
        typeof arg === 'string' ? `'${arg}'` : arg
    ).join(', ');
    const DESCRIPTION = `
## Sample Cloudwatch metric filterer.

Filters metrics whose values match a condition, such as show only metrics where average of all values > 70. 

This enables use cases like:
* Show only problematic resources on my dashboard, e.g. graph only EC2 instances with high CPU
* Alarm on sum of all metrics matching a CloudWatch [metricsearch expression](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/search-expression-syntax.html)

Runs a valid CloudWatch Metric expression and returns all metrics that match the specified filter expression. Leave the filter expression blank to match all metrics.

### Query arguments

\\# | Type | Description
---|---|---
1 | String | Expression, e.g. CPU for EC2 instances: \`SEARCH('{AWS/EC2,InstanceId} MetricName=CPUUtilization)', 'Average')\`
2 | String | Filter, in form '<stat> <condition> <value>', where <stat> can be MIN,MAX,AVG, SUM and <condition> can be >, >=, <, <=, ==, !=. e.g. \`MAX > 70\` shows only metrics that have some datapoints above 70

### Example Expression
Display only EC2 Instances where CPU went over 70%:

\`\`\`
LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', ${FIRST_EXAMPLE_DEFAULT_ARGS_STRING})

\`\`\`

Average CPU of all EC2 Instances using SEARCH, by setting filter to empty string. This metric can be alarmed on.

\`\`\`
AVG(LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', 'SEARCH("{AWS/EC2,InstanceId} MetricName=CPUUtilization)", "Average"))', '')
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

const VALID_STATS = new Set(['MIN', 'MAX', 'AVG', 'SUM']);
const VALID_CONDITIONS = new Set(['>', '<', '==', '>=', '<=', '!=']);

const parseFilter = (filterString) => {
    const filterParts = filterString.replace(/ +/g, ' ').trim().split(' ');
    if (filterParts.length === 1 && filterParts[0] === '') {
        return { stat: null };
    }
    if (filterParts.length !== 3) {
        throw Error(
            `Filter syntax error, '${filterString}' does not follow format '<stat> <condition> <value>' or empty, ''`,
            { cause: 'Validation' }
        );
    }

    const [stat, condition, valueString] = filterParts;
    const value = parseFloat(valueString);

    if (!VALID_STATS.has(stat)) {
        throw Error(`Unrecognised stat, '${stat}' in filter`, { cause: 'Validation' });
    }
    if (!VALID_CONDITIONS.has(condition)) {
        throw Error(`Unrecognised condition, '${condition}' in filter`, { cause: 'Validation' });
    }

    return { stat, condition, value };
};

const validateGetMetricDataRequest = (Arguments) => {
    if (Arguments.length !== 2) {
        throw Error(`Expected 2 arguments, received ${Arguments.length}`, { cause: 'Validation' });
    }
    const expression = Arguments[0];
    const filterString = Arguments[1];

    if (typeof expression !== 'string' || typeof filterString !== 'string') {
        throw Error(`Unexpected argument type, expected (<string>, <string>)`, { cause: 'Validation' });
    }
    const filter = parseFilter(filterString);

    return { expression, filter };
};

const statMatchesFilter = (values, filter) => {
    // Check for empty filter, return true
    if (filter.stat === null) {
        return true;
    }

    // Return false for no data metric, remove them
    if (values.length === 0) {
        return false;
    }

    let min = values[0];
    let max = min;
    let sum = min;

    values.slice(1).forEach((value) => {
        sum += value;
        if (value < min) {
            min = value;
        } else if (value > max) {
            max = value;
        }
    });

    const avg = sum / values.length;
    const statMap = { AVG: avg, MIN: min, MAX: max, SUM: sum };
    const stat = statMap[filter.stat]; // Extract the stat to apply to the filter condition
    switch (filter.condition) {
        case '>':
            return stat > filter.value;
        case '>=':
            return stat >= filter.value;
        case '<':
            return stat < filter.value;
        case '<=':
            return stat <= filter.value;
        case '==':
            return stat === filter.value;
        case '!=':
            return stat !== filter.value;
        default:
            return false;
    }
};

const getMetricDataEventHandler = async (event) => {
    const { StartTime, EndTime, Period, Arguments } = event.GetMetricDataRequest;
    const { expression, filter } = validateGetMetricDataRequest(Arguments);
    const getMetricDataParams = {
        MetricDataQueries: [{ Id: 'e1', Expression: expression, Period }],
        StartTime: new Date(StartTime * 1000),
        EndTime: new Date(EndTime * 1000),
    };
    const { region } = event;
    const cloudwatch = new cw.CloudWatch({ region });
    const getMetricDataResult = await cloudwatch.getMetricData(getMetricDataParams);
    const metrics = getMetricDataResult.MetricDataResults;
    const filteredMetrics = metrics.filter((metric) => {
        if (statMatchesFilter(metric.Values, filter)) {
            // Filter matches, so convert timestamps to expected format and return true
            /* eslint no-param-reassign: ["error", { "props": false }] */
            metric.Timestamps = metric.Timestamps.map((ts) => {
                return ts.getTime() / 1000;
            });
            return true;
        }
        return false;
    });
    return { MetricDataResults: filteredMetrics };
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

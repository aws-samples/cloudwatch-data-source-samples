const cw = require('@aws-sdk/client-cloudwatch');

const describeGetMetricDataEventHandler = () => {
    const FIRST_EXAMPLE_DEFAULT_ARGS_LIST = [
        'AWS/Usage, CallCount, Type, API, Resource, GetMetricData, Service, CloudWatch, Class, None',
        'Sum',
        'P1D',
        7,
    ];
    const FIRST_EXAMPLE_DEFAULT_ARGS_STRING = FIRST_EXAMPLE_DEFAULT_ARGS_LIST.map((arg) =>
        typeof arg === 'string' ? `'${arg}'` : arg
    ).join(', ');
    const DESCRIPTION = `
## Sample Cloudwatch Metric Timeshift data source connector

"Time shifts" a CloudWatch Metric, to show how a metric behaves now compared to periodic times in the past. 
It also enables alarming on data from up to 15 months ago.

### Query arguments

\\# | Type | Description
---|---|---
1 | String | The full name of the metric, in format <Namespace>, <MetricName>, <Dim Name 1>, <Dim Value 1>,... etc. URL encode the strings between commas
2 | String | The statistic to retrieve for the metric
3 | String | The shift interval, in ISO 8601 duration format, e.g. P7D for 1 week, PT3H for 3 hours
4 | Number | The number of shifts to perform, between 1 and 10

### Example Expression
Plot number of calls to CloudWatch GetMetricData, day over day for past 8 days (current, plus 7 timeshifts of 1 day)

\`\`\`
LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', ${FIRST_EXAMPLE_DEFAULT_ARGS_STRING})
\`\`\`

Compare number of calls to CloudWatch GetMetricData today versus a week ago, as percent of how it has changed - which can be alarmed on. This is done with 4 expressions on the graph, listed below with their metric ids. The final "metric", **percentChange**, calculates the percentage change - alarm on this, and set the other "metrics" to invisible.

\`\`\`
timeshift = LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', 'AWS/Usage, CallCount, Type, API, Resource, GetMetricData, Service, CloudWatch, Class, None', 'Sum', 'P7D', 1)
current = FIRST(timeshift)
previous = LAST(timeshift)
percentChange = IF(previous != 0, current / previous * 100)
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

const parseISO8601DurationString = (durationString) => {
    const stringPattern = /^P(?:(\d+)D)?[T]*(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d{1,3})?)S)?$/;
    const stringParts = stringPattern.exec(durationString);
    if (stringParts === null) {
        throw Error(`Unrecognized ISO duration ${durationString}`, { cause: 'Validation' });
    }
    const days = stringParts[1] === undefined ? 0 : parseInt(stringParts[1], 10);
    const hours = stringParts[2] === undefined ? 0 : parseInt(stringParts[2], 10);
    const mins = stringParts[3] === undefined ? 0 : parseInt(stringParts[3], 10);
    const secs = stringParts[4] === undefined ? 0 : parseInt(stringParts[4], 10);

    return ((days * 24 + hours) * 60 + mins) * 60 + secs;
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

const convertSecondsToHumanReadable = (seconds) => {
    const d = Math.floor(seconds / (3600 * 24));
    const h = Math.floor((seconds % (3600 * 24)) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);

    const dDisplay = d > 0 ? `${d}d ` : '';
    const hDisplay = h > 0 ? `${h}h ` : '';
    const mDisplay = m > 0 ? `${m}m ` : '';
    const sDisplay = s > 0 ? `${s}s ` : '';
    return (dDisplay + hDisplay + mDisplay + sDisplay).trim();
};

const validateGetMetricDataRequest = (Arguments) => {
    if (Arguments.length !== 4) {
        throw Error(`Expected 4 arguments, received ${Arguments.length}`, { cause: 'Validation' });
    }
    const [fullMetric, stat, shiftIntervalString, numberOfShifts] = Arguments;

    if (
        typeof fullMetric !== 'string' ||
        typeof stat !== 'string' ||
        typeof shiftIntervalString !== 'string' ||
        typeof numberOfShifts !== 'number'
    ) {
        throw Error(`Unexpected argument type, expected (<string>, <string>, <string>, <number>)`, {
            cause: 'Validation',
        });
    }
    const shiftInterval = parseISO8601DurationString(shiftIntervalString);
    if (shiftInterval <= 0) {
        throw Error(`Illegal shift interval '${shiftIntervalString}' specified, must be > 0 seconds`, {
            cause: 'Validation',
        });
    }
    if (numberOfShifts < 1 || numberOfShifts > 10) {
        throw Error(`Number of shifts, ${numberOfShifts}, must be between 1 and 10 inclusive`, {
            cause: 'Validation',
        });
    }

    const metric = parseFullMetric(fullMetric);

    return { metric, stat, shiftInterval, numberOfShifts };
};

const shiftFullMetricData = (metricStat, fullMetricData, StartTime, EndTime, Period, shiftInterval, numberOfShifts) => {
    const fullMetricDataMap = {};
    fullMetricData.Timestamps.forEach((ts, index) => {
        fullMetricDataMap[ts.toISOString()] = fullMetricData.Values[index];
    });
    const metrics = [];

    for (let i = 0; i <= numberOfShifts; i++) {
        const timeOffset = i * shiftInterval;
        const metric = {
            Status: 'Complete',
            Label: i === 0 ? 'current' : `- ${convertSecondsToHumanReadable(timeOffset)}`,
            Timestamps: [],
            Values: [],
        };
        for (let time = StartTime; time < EndTime; time += Period) {
            const shiftedTimestamp = time - timeOffset;
            const shiftedIsoTimestamp = new Date(shiftedTimestamp * 1000).toISOString();
            const value = fullMetricDataMap[shiftedIsoTimestamp];
            if (value !== undefined) {
                metric.Timestamps.push(time);
                metric.Values.push(value);
            }
        }
        metrics.push(metric);
    }
    return { MetricDataResults: metrics };
};

const getMetricDataEventHandler = async (event) => {
    const { StartTime, EndTime, Period, Arguments } = event.GetMetricDataRequest;
    const { metric, stat, shiftInterval, numberOfShifts } = validateGetMetricDataRequest(Arguments);
    const metricStat = {
        Metric: metric,
        Stat: stat,
        Period,
    };
    const roundedStart = StartTime - (StartTime % Period);
    const fullShiftStart = roundedStart - shiftInterval * numberOfShifts;
    const gmdParams = {
        MetricDataQueries: [{ Id: 'm1', MetricStat: metricStat }],
        StartTime: new Date(fullShiftStart * 1000),
        EndTime: new Date(EndTime * 1000),
    };
    const { region } = event;

    const cloudwatch = new cw.CloudWatch({ region });
    const gmdResponse = await cloudwatch.getMetricData(gmdParams);
    const data = gmdResponse.MetricDataResults;
    const fullMetricData = data[0];
    const timeshiftedData = shiftFullMetricData(
        metricStat,
        fullMetricData,
        roundedStart,
        EndTime,
        Period,
        shiftInterval,
        numberOfShifts
    );
    return timeshiftedData;
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

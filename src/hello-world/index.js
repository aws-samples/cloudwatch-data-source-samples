const getValidationError = (message) => {
    return {
        Error: {
            Code: 'Validation',
            Value: message,
        },
    };
};

const validateGetMetricDataRequest = (event) => {
    const { Arguments } = event.GetMetricDataRequest;

    if (Arguments.length !== 2) {
        return getValidationError('Argument count must be 2');
    }

    const metricName = Arguments[0];
    const value = Arguments[1];

    if (typeof metricName !== 'string') {
        return getValidationError('Argument 1 must be a string');
    }

    if (typeof value !== 'number') {
        return getValidationError('Argument 2 must be a number');
    }

    return null;
};

const getMetricDataEventHandler = (event) => {
    const validationError = validateGetMetricDataRequest(event);

    if (validationError) {
        return validationError;
    }

    const { StartTime, EndTime, Period, Arguments } = event.GetMetricDataRequest;
    const metricName = Arguments[0];
    const value = Arguments[1];

    const data = [];
    let currentTime = StartTime;
    while (currentTime < EndTime) {
        const dataPoint = {
            timestamp: currentTime,
            value,
        };

        data.push(dataPoint);

        currentTime += Period;
    }

    return {
        MetricDataResults: [
            {
                StatusCode: 'Complete',
                Label: metricName,
                Timestamps: data.map((val) => val.timestamp),
                Values: data.map((val) => val.value),
            },
        ],
    };
};

const describeGetMetricDataEventHandler = () => {
    const description = `
## Sample hello world data source connector

Generates a sample time series at a given value across a time range

### Query arguments

\\# | Type | Description
---|---|---
1 | String | The name of the time series
2 | Number | The value returned for all data points in the time series

### Example Expression

\`\`\`
LAMBDA('${process.env.AWS_LAMBDA_FUNCTION_NAME}', 'metricLabel', 10)
\`\`\`
`;
    return {
        DataSourceConnectorName: 'Echo',
        ArgumentDefaults: [{ Value: 'metricLabel' }, { Value: 10 }],
        Description: description,
    };
};

exports.handler = async (event) => {
    switch (event.EventType) {
        case 'GetMetricData':
            return getMetricDataEventHandler(event);
        case 'DescribeGetMetricData':
            return describeGetMetricDataEventHandler(event);
        default:
            return getValidationError(`Unknown EventType: ${event.EventType}`);
    }
};

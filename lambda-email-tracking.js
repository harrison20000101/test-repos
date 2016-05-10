/**
 * Created by hliu on 4/28/16.
 */
console.log('Loading function');

var DEFAULT_AWS_REGION = 'us-east-1';
var DEFAULT_AWS_API_VERSION = 'latest';
var DEFAULT_AWS_FIREHOSE_ACCESS_KEY_ID = 'AKIAIXRA7XLYKKAHQQQQ';
var DEFAULT_AWS_FIREHOSE_SECRET_ACCESS_KEY = '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';
var DEFAULT_AWS_FIREHOSE_DELIMITER = '|';
var DEFAULT_AWS_FIREHOSE_EOL = '\n';

var SENDGRID_TRACKING_EMAIL = 1;
var SENDGRID_TRACKING_CLICK = 2;
var SENDGRID_TRACKING_PIXEL = 3;
var SENDGRID_TRACKING_SENT = 4;

var AWS_FIREHOSE_STREAM_EMAIL = 'edit-email-dev-email';
var AWS_FIREHOSE_STREAM_CLICK = 'edit-email-dev-click';
var AWS_FIREHOSE_STREAM_PIXEL = 'edit-email-dev-pixel';
var AWS_FIREHOSE_STREAM_SENT = 'edit-email-dev-sent';

var debug = true;
var online = false;
var aws = require('aws-sdk');
var setRegion = process.env.AWS_REGION;
var setAPIVersion = DEFAULT_AWS_API_VERSION;//'2015-08-04';

var firehose;
exports.firehose = firehose;

function setup() {
    if (debug) {
        console.log("Connecting to Amazon Kinesis Firehose in " + setRegion);
    }

    // configure a new connection to firehose, if one has not been provided
    if (!exports.firehose) {
        exports.firehose = new aws.Firehose({
            apiVersion: setAPIVersion,
            region: setRegion,
            accessKeyId: DEFAULT_AWS_FIREHOSE_ACCESS_KEY_ID,
            secretAccessKey: DEFAULT_AWS_FIREHOSE_SECRET_ACCESS_KEY
        });
    }
}

exports.firehoseProcess = function(event, context) {
    var sendgrid_event = event[0];
    var type = sendgrid_event.event;
    var time = sendgrid_event.timestamp;
    var uniqueArgs = sendgrid_event.UniqueArgs;

    if (debug) {
        console.log('SendGrid uniqueArgs:', uniqueArgs);
    }

    var parseTrackInfo = function(trackingType, trackingEvent) {
        if (debug) {
            console.log('parseTrackInfo type:', trackingType);
        }
        var tracking_data = [];
        var result = null;
        //var time = Math.round(new Date() / 1000);
        var uniqueArgs = JSON.parse(trackingEvent.UniqueArgs);

        if (trackingType == SENDGRID_TRACKING_SENT) {
            result = [];
            sent_map = uniqueArgs.sent_map;
            for (var i = 0; i < sent_map.length; i++) {
                var sent_args = JSON.parse(sent_map[i]);
                sent_unit = [
                    sent_args.time_created,
                    sent_args.esg_id,
                    sent_args.editorial_id,
                    sent_args.placement,
                    uniqueArgs.digest_name
                ];
                sent_unit_string = sent_unit.join(DEFAULT_AWS_FIREHOSE_DELIMITER) + DEFAULT_AWS_FIREHOSE_EOL;
                result.push(sent_unit_string);
            }
        }
        else {
            switch (trackingType) {
                case SENDGRID_TRACKING_EMAIL:
                    tracking_data = [
                        uniqueArgs.esg_id,
                        uniqueArgs.status,
                        uniqueArgs.template_name,
                        uniqueArgs.time_created,
                        uniqueArgs.flags,
                        uniqueArgs.email_src,
                        uniqueArgs.subject,
                        uniqueArgs.entity_id,
                        uniqueArgs.entity_type,
                    ];
                    break;

                case SENDGRID_TRACKING_CLICK:
                    var url = require('url');
                    var urlParams = url.parse(trackingEvent.url, true);

                    tracking_data = [
                        uniqueArgs.esg_id,
                        uniqueArgs.template_name,
                        time,
                        urlParams.query.trk,
                        urlParams.query.trk_info,
                        0,//session id
                        uniqueArgs.email_src,
                        urlParams.query.flags,
                    ];
                    break;

                case SENDGRID_TRACKING_PIXEL:
                    tracking_data = [
                        uniqueArgs.esg_id,
                        uniqueArgs.template_name,
                        time,
                        uniqueArgs.email_src,
                    ];
                    break;

                default:
                    break;
            }

            if (tracking_data.length) {
                tracking_data.push(uniqueArgs.digest_name);
                result = tracking_data.join(DEFAULT_AWS_FIREHOSE_DELIMITER) + DEFAULT_AWS_FIREHOSE_EOL;
            }
        }

        if (debug) {
            console.log('tracking_data:', result);
        }

        return result;
    };

    var emailTrackingpHandler = function(event) {
        switch (event.event) {
            case 'processed':
                var email_tracking = parseTrackInfo(SENDGRID_TRACKING_EMAIL, event);
                if (!!email_tracking) {
                    exports.writeToFirehose(AWS_FIREHOSE_STREAM_EMAIL, email_tracking);
                }

                var sent_map_trackings = parseTrackInfo(SENDGRID_TRACKING_SENT, event);
                for (var i = 0; i < sent_map_trackings.length; i++) {
                    exports.writeToFirehose(AWS_FIREHOSE_STREAM_SENT, sent_map_trackings[i]);
                }

                break;
            case 'open':
                var pixel_tracking = parseTrackInfo(SENDGRID_TRACKING_PIXEL, event);
                if (!!pixel_tracking) {
                    exports.writeToFirehose(AWS_FIREHOSE_STREAM_PIXEL, pixel_tracking);
                }
                break;
            case 'click':
                var click_tracking = parseTrackInfo(SENDGRID_TRACKING_CLICK, event);
                if (!!click_tracking) {
                    exports.writeToFirehose(AWS_FIREHOSE_STREAM_CLICK, click_tracking);
                }
                break;
            default:
                break;
        }
    };

    exports.writeToFirehose = function(deliveryStreamName, stream) {
        var result;
        var params = {
            DeliveryStreamName: deliveryStreamName, // REQUIRED
            Record: {
                Data: stream,
            }
        };

        if (debug) {
            console.log('Writing stream to firehose : ', JSON.stringify(params, null, 2));
        }

        exports.firehose.putRecord(params, function(err, data) {
            if (err) {
                if (debug) {
                    console.log(err, err.stack); // an error occurred
                }
            } else {
                if (debug) {
                    console.log(data);           // successful response
                }
            }
        });
    };

    emailTrackingpHandler(sendgrid_event);
};

exports.handler = function(event, context) {
    if (debug) {
        console.log("SendGrid Webhook Event : ", JSON.stringify(event, null, 2));
    }
    setup();
    exports.firehoseProcess(event, context);
};

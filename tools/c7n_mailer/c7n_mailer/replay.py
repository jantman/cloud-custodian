"""
Allow local testing of mailer and templates by replaying an SQS message.

MAILER_FILE input is a file containing the exact base64-encoded, gzipped
data that's enqueued to SQS via :py:meth:`c7n.actions.Notify.send_sqs`.

Alternatively, with -p|--plain specified, the file will be assumed to be
JSON data that can be loaded directly.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
import boto3
import os
import logging
import zlib
import base64
import json

import jsonschema
import yaml

from c7n_mailer.utils import setup_defaults
from c7n_mailer.cli import CONFIG_SCHEMA
from .email_delivery import EmailDelivery

logger = logging.getLogger(__name__)


class MailerTester(object):

    def __init__(self, msg_file, config, msg_plain=False):
        if not os.path.exists(msg_file):
            raise RuntimeError("File does not exist: %s" % msg_file)
        logger.debug('Reading message from: %s', msg_file)
        with open(msg_file, 'r') as fh:
            raw = fh.read()
        logger.debug('Read %d byte message', len(raw))
        if msg_plain:
            raw = raw.strip()
        else:
            logger.debug('base64-decoding and zlib decompressing message')
            raw = zlib.decompress(base64.b64decode(raw))
            logger.debug('Raw JSON string: %s', raw)
        self.data = json.loads(raw)
        logger.debug('Loaded message JSON')
        self.config = config
        self.session = boto3.Session()

    def run(self, dry_run=False, print_only=False):
        emd = EmailDelivery(self.config, self.session, None)
        addrs_to_msgs = emd.get_to_addrs_email_messages_map(self.data)
        logger.info('Would send email to: %s', addrs_to_msgs.keys())
        if print_only:
            mime = emd.get_mimetext_message(
                self.data, self.data['resources'], ['foo@example.com']
            )
            logger.info('Send mail with subject: "%s"', mime['Subject'])
            print(mime.get_payload())
            return
        if dry_run:
            for to_addrs, mimetext_msg in addrs_to_msgs.items():
                print('-> SEND MESSAGE TO: %s' % to_addrs)
                print(mimetext_msg)
            return
        # else actually send the message...
        for to_addrs, mimetext_msg in addrs_to_msgs.items():
            logger.info('Actually sending mail to: %s', to_addrs)
            emd.send_c7n_email(self.data, list(to_addrs), mimetext_msg)


def setup_parser():
    parser = argparse.ArgumentParser('Test c7n-mailer templates and mail')
    parser.add_argument('-c', '--config', required=True)
    parser.add_argument('-d', '--dry-run', dest='dry_run', action='store_true',
                        default=False,
                        help='Log messages that would be sent, but do not send')
    parser.add_argument('-t', '--template-print', dest='print_only',
                        action='store_true', default=False,
                        help='Just print rendered templates')
    parser.add_argument('-p', '--plain', dest='plain', action='store_true',
                        default=False,
                        help='Expect MESSAGE_FILE to be a plain string, '
                             'rather than the base64-encoded, gzipped SQS '
                             'message format')
    parser.add_argument('MESSAGE_FILE', type=str,
                        help='Path to SQS message dump/content file')
    return parser


def session_factory(config):
    return boto3.Session(
        region_name=config['region'],
        profile_name=config.get('profile'))


def main():
    parser = setup_parser()
    options = parser.parse_args()

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    with open(options.config) as fh:
        config = yaml.load(fh.read(), Loader=yaml.SafeLoader)

    jsonschema.validate(config, CONFIG_SCHEMA)
    setup_defaults(config)

    tester = MailerTester(options.MESSAGE_FILE, config, msg_plain=options.plain)
    tester.run(options.dry_run, options.print_only)


if __name__ == '__main__':
    main()

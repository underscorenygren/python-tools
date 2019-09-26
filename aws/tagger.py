import argparse
import logging

import boto3


class Service(object):
	def __init__(self, service_name):
		self.service_name = service_name
		self.cli = boto3.client(service_name)
		self.region_name = boto3.session.Session().region_name

	def paginator(self, fn_name, **kwargs):
		pager = self.cli.get_paginator(fn_name)
		for page in pager.paginate(**kwargs):
			yield page


class Lambda(Service):
	def __init__(self):
		super().__init__('lambda')

	def entries(self):
		for entry in self.paginator('list_functions'):
			yield entry

	def items(self, resp):
		return resp['Functions']

	def arn(self, item):
		return item['FunctionArn']

	def name(self, item):
		return item['FunctionName']

	def get_tags(self, item):
		resp = self.cli.list_tags(Resource=self.arn(item))
		return resp.get("Tags", {})

	def new_tags(self, item):
		return {
			"Name": self.name(item)
		}

	def tag_it(self, arn, tags):
		self.cli.tag_resource(Resource=arn, Tags=new_tags)


class APIGateway(Service):
	def __init__(self):
		super().__init__('apigateway')

	def entries(self):
		for entry in self.paginator('get_rest_apis'):
			yield entry

	def items(self, resp):
		return resp['items']

	def _id(self, item):
		return item['id']

	def arn(self, item):
		_id = self._id(item)
		return f"arn:aws:apigateway:{self.region_name}::/restapis/{_id}"

	def name(self, item):
		return item['name']

	def get_tags(self, item):
		return item.get('tags', self.cli.get_tags(resourceArn=self.arn(item))['tags'])

	def new_tags(self, item):
		return {
			"Name": self.name(item)
		}

	def tag_it(self, arn, tags):
		self.cli.tag_resource(resourceArn=arn, tags=tags)


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

# overwrites existing tags
MODE_OVERWRITE = 'overwrite'
# adds any non-existing tags
MODE_ADD = 'add'
# only adds tags if object non-empty
MODE_CREATE_ONLY = 'create-only'
MODES = [MODE_OVERWRITE, MODE_ADD, MODE_CREATE_ONLY]

def _default_filterfn(svc, item):
	return True


def tag(svc, mode=MODE_CREATE_ONLY, filterfn=_default_filterfn, dry_run=False):
	updates = []

	for resp in svc.entries():
		items = svc.items(resp)
		for item in filter(lambda i: filterfn(svc, i), items):
			arn = svc.arn(item)
			existing_tags = svc.get_tags(item)
			new_tags = svc.new_tags(item)
			if not new_tags:
				logger.debug(f"no new_tags for {item}")
			elif mode == MODE_ADD:
				new_tags = {key: val for (key, val) in new_tags.items() if key not in existing_tags}
				updates.append((arn, new_tags))
			elif mode == MODE_OVERWRITE:
				updates.append((arn, new_tags))
			elif mode == MODE_CREATE_ONLY:
				if not existing_tags:
					updates.append((arn, new_tags))
				else:
					logger.debug(f"not overwriting existing tags {existing_tags} on {arn}")

	for arn, new_tags in updates:
		logger.debug(f"adding tags {new_tags} to {arn}")
		if not dry_run:
			svc.tag_it(arn, new_tags)


if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="tags amazon resource names")

	klasses = [subclass.__name__ for subclass in Service.__subclasses__()]

	parser.add_argument('service', type=str, choices=klasses, help="The service to tag")
	parser.add_argument('--dry-run', action="store_true", help="lists resources and tags, but doesn't actually perform tagging")
	parser.add_argument('--mode', type=str, choices=MODES, default=MODE_CREATE_ONLY, help="what to do with existing tags: overwrite, add if not present, or skip if any present")

	args = parser.parse_args()

	cls = globals()[args.service]

	tag(cls(), mode=args.mode, dry_run=args.dry_run)

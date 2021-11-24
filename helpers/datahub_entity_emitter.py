# %%

import requests
from datahub.emitter.mcp import MetadataChangeProposalWrapper
import datahub.metadata.schema_classes as models
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter


# %%


class DatahubEntityEmitter(DatahubRestEmitter):

    def __init__(self, env='DEV', datahub_actor='urn:li:corpuser:admin', dataset_platform='bigquery',
                 dashboard_platform='bigquery', *args, **kwargs):
        super(DatahubEntityEmitter, self).__init__(*args, **kwargs)
        self.env = env
        self.datahub_gms_server = kwargs.get('gms_server', None)
        self.datahub_token = kwargs.get('token', None)
        self.datahub_actor = datahub_actor
        self.dataset_platform = dataset_platform
        self.dashboard_platform = dashboard_platform

    def get_change_type(self, change_type):
        if change_type == 'upsert':
            return models.ChangeTypeClass.UPSERT
        elif change_type == 'update':
            return models.ChangeTypeClass.UPDATE
        elif change_type == 'create':
            return models.ChangeTypeClass.CREATE
        elif change_type == 'delete':
            return models.ChangeTypeClass.DELETE
        else:
            return models.ChangeTypeClass.UPSERT

    def update_glossary_term(self, name, definition='', source='INTERNAL', change_type='upsert'):
        mcpw = MetadataChangeProposalWrapper(
            "glossaryTerm",
            self.get_change_type(change_type),
            entityUrn=f'urn:li:glossaryTerm:{name}',
            aspectName="glossaryTermInfo",
            aspect=models.GlossaryTermInfoClass(
                definition=definition,
                termSource=source
            )
        )
        self.emit_mcp(mcp=mcpw)

    def update_user(self, name, email='', display_name=None, active=True, change_type='upsert'):

        mcpw = MetadataChangeProposalWrapper(
            "corpUser",
            self.get_change_type(change_type),
            entityUrn=builder.make_user_urn(username=name),
            aspectName="corpUserInfo",
            aspect=models.CorpUserInfoClass(
                displayName=display_name if display_name else name,
                email=email,
                active=active
            )
        )
        self.emit_mcp(mcp=mcpw)

    def update_tag(self, name, description='', change_type='upsert'):

        mcpw = MetadataChangeProposalWrapper(
            "tag",
            self.get_change_type(change_type),
            entityUrn=builder.make_tag_urn(name),
            aspectName="tagProperties",
            aspect=models.TagPropertiesClass(
                name=name,
                description=description
            )
        )
        self.emit_mcp(mcp=mcpw)

    def update_dataset(
            self, name, platform='bigquery', env='DEV', description=None, url=None, tags=None, change_type='upsert',
            owners=None, custom_properties=None,
            glossary_terms=None, upstream_datasets=None, links=None
    ):

        dataset_urn = builder.make_dataset_urn(platform=platform, name=name, env=env)

        mcpw = MetadataChangeProposalWrapper(
            "dataset",
            self.get_change_type(change_type),
            entityUrn=dataset_urn,
            aspectName="datasetProperties",
            aspect=models.DatasetPropertiesClass(
                description=description,
                externalUrl=url
            )
        )
        self.emit_mcp(mcp=mcpw)

        # add tags
        if tags:
            mcpw = MetadataChangeProposalWrapper(
                "dataset",
                self.get_change_type(change_type),
                entityUrn=dataset_urn,
                aspectName="globalTags",
                aspect=models.GlobalTagsClass(
                    tags=[models.TagAssociationClass(builder.make_tag_urn(tag)) for tag in tags])
            )
            self.emit_mcp(mcp=mcpw)

        # add owners
        if owners:
            mcpw = MetadataChangeProposalWrapper(
                "dataset",
                self.get_change_type(change_type),
                entityUrn=dataset_urn,
                aspectName="ownership",
                aspect=models.OwnershipClass(
                    owners=[
                        models.OwnerClass(builder.make_user_urn(owner), type='DATAOWNER')
                        for owner in owners
                    ]
                )
            )
            self.emit_mcp(mcp=mcpw)

        # add custom properties
        if custom_properties:
            mcpw = MetadataChangeProposalWrapper(
                "dataset",
                self.get_change_type(change_type),
                entityUrn=dataset_urn,
                aspectName="datasetProperties",
                aspect=models.DatasetPropertiesClass(
                    customProperties=custom_properties
                )
            )
            self.emit_mcp(mcp=mcpw)

        # add glossary terms
        if glossary_terms:
            mcpw = MetadataChangeProposalWrapper(
                "dataset",
                self.get_change_type(change_type),
                entityUrn=dataset_urn,
                aspectName="glossaryTerms",
                aspect=models.GlossaryTermsClass(
                    terms=[
                        models.GlossaryTermAssociationClass(f'urn:li:glossaryTerm:{term}')
                        for term in glossary_terms
                    ],
                    auditStamp=models.AuditStampClass(time=0, actor=self.datahub_actor)
                )
            )
            self.emit_mcp(mcp=mcpw)

        # add upstream datasets
        if upstream_datasets:
            mcpw = MetadataChangeProposalWrapper(
                "dataset",
                self.get_change_type(change_type),
                entityUrn=dataset_urn,
                aspectName="upstreamLineage",
                aspect=models.UpstreamLineageClass(
                    upstreams=[
                        models.UpstreamClass(
                            dataset=builder.make_dataset_urn(
                                platform=platform,
                                name=dataset,
                                env=env
                            ),
                            type=models.DatasetLineageTypeClass.TRANSFORMED
                        )
                        for dataset in upstream_datasets
                    ]
                )
            )
            self.emit_mcp(mcp=mcpw)

        # add links
        if links:
            mcpw = MetadataChangeProposalWrapper(
                "dataset",
                self.get_change_type(change_type),
                entityUrn=dataset_urn,
                aspectName="institutionalMemory",
                aspect=models.InstitutionalMemoryClass(
                    elements=[
                        models.InstitutionalMemoryMetadataClass(
                            url=links[link],
                            description=link,
                            createStamp=models.AuditStampClass(time=0, actor=self.datahub_actor)
                        )
                        for link in links
                    ]
                )
            )
            self.emit_mcp(mcp=mcpw)

    def update_dashboard(
            self, name, inputs=None, title=None, description=None, change_type='upsert', platform='datastudio',
            tags=None, owners=None, glossary_terms=None,
            links=None, url=None
    ):

        if inputs is None:
            inputs = []

        chart_urn = builder.make_chart_urn(platform=platform, name=name)
        dashboard_urn = builder.make_dashboard_urn(platform=platform, name=name)

        # create chart
        mcpw = MetadataChangeProposalWrapper(
            "chart",
            self.get_change_type(change_type),
            entityUrn=chart_urn,
            aspectName="chartInfo",
            aspect=models.ChartInfoClass(
                title=title if title else name,
                description=description,
                externalUrl=url,
                lastModified=models.ChangeAuditStampsClass(
                    created=models.AuditStampClass(
                        time=0,
                        actor=self.datahub_actor
                    )
                ),
                inputs=[
                    builder.make_dataset_urn(
                        platform=self.dataset_platform,
                        name=input,
                        env=self.env
                    )
                    for input in inputs
                ]
            )
        )
        self.emit_mcp(mcp=mcpw)

        # create dashboard
        mcpw = MetadataChangeProposalWrapper(
            "dashboard",
            self.get_change_type(change_type),
            entityUrn=dashboard_urn,
            aspectName="dashboardInfo",
            aspect=models.DashboardInfoClass(
                title=title if title else name,
                description=description,
                externalUrl=url,
                charts=[chart_urn],
                lastModified=models.ChangeAuditStampsClass(
                    created=models.AuditStampClass(
                        time=0,
                        actor=self.datahub_actor
                    )
                )
            )
        )
        self.emit_mcp(mcp=mcpw)

        # add tags
        if tags:
            mcpw = MetadataChangeProposalWrapper(
                "dashboard",
                self.get_change_type(change_type),
                entityUrn=dashboard_urn,
                aspectName="globalTags",
                aspect=models.GlobalTagsClass(
                    tags=[models.TagAssociationClass(builder.make_tag_urn(tag)) for tag in tags])
            )
            self.emit_mcp(mcp=mcpw)

        # add owners
        if owners:
            mcpw = MetadataChangeProposalWrapper(
                "dashboard",
                self.get_change_type(change_type),
                entityUrn=dashboard_urn,
                aspectName="ownership",
                aspect=models.OwnershipClass(
                    owners=[
                        models.OwnerClass(builder.make_user_urn(owner), type='DATAOWNER')
                        for owner in owners
                    ]
                )
            )
            self.emit_mcp(mcp=mcpw)

        # add glossary terms
        if glossary_terms:
            mcpw = MetadataChangeProposalWrapper(
                "dashboard",
                self.get_change_type(change_type),
                entityUrn=dashboard_urn,
                aspectName="glossaryTerms",
                aspect=models.GlossaryTermsClass(
                    terms=[
                        models.GlossaryTermAssociationClass(f'urn:li:glossaryTerm:{term}')
                        for term in glossary_terms
                    ],
                    auditStamp=models.AuditStampClass(time=0, actor=self.datahub_actor)
                )
            )
            self.emit_mcp(mcp=mcpw)

        # add links
        if links:
            mcpw = MetadataChangeProposalWrapper(
                "dashboard",
                self.get_change_type(change_type),
                entityUrn=dashboard_urn,
                aspectName="institutionalMemory",
                aspect=models.InstitutionalMemoryClass(
                    elements=[
                        models.InstitutionalMemoryMetadataClass(
                            url=links[link],
                            description=link,
                            createStamp=models.AuditStampClass(time=0, actor=self.datahub_actor)
                        )
                        for link in links
                    ]
                )
            )
            self.emit_mcp(mcp=mcpw)

    def dataset_add_tag(self, name, tag):

        resource_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.dataset_platform},{name},{self.env})"
        url = f"{self.datahub_gms_server}/api/graphql"
        headers = {
            f'Authorization': f'Bearer {self.datahub_token}',
            'X-DataHub-Actor': self.datahub_actor
        }

        graphql_query_add_tag = {
            "query": """mutation addTag($input: TagAssociationInput!) {\n
                addTag(input: $input)
            }""",
            "variables": {
                "input": {
                    "tagUrn": f"urn:li:tag:{tag}",
                    "resourceUrn": resource_urn
                }
            }
        }

        requests.post(
            url,
            json=graphql_query_add_tag,
            headers=headers
        )

    def dataset_remove_tag(self, name, tag):

        resource_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.dataset_platform},{name},{self.env})"
        url = f"{self.datahub_gms_server}/api/graphql"
        headers = {
            f'Authorization': f'Bearer {self.datahub_token}',
            'X-DataHub-Actor': self.datahub_actor
        }

        graphql_query_remove_tag = {
            "query": """mutation removeTag($input: TagAssociationInput!) {\n
                removeTag(input: $input)
            }""",
            "variables": {
                "input": {
                    "tagUrn": f"urn:li:tag:{tag}",
                    "resourceUrn": resource_urn
                }
            }
        }

        requests.post(
            url,
            json=graphql_query_remove_tag,
            headers=headers
        )


# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: spec
   :synopsis: This package provides the configuration for a Warabi provider
   and the corresponding ConfigurationSpace.

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


from dataclasses import dataclass
from typing import Optional
from dataclasses import dataclass
from mochi.bedrock.spec import ProviderSpec


@dataclass(frozen=True)
class BackendType:

    name: str
    is_persistent: bool = True


class WarabiProviderSpec(ProviderSpec):

    _backends = [
        BackendType(name='memory', is_persistent=False),
        BackendType(name='abtio'),
        BackendType(name='pmdk'),
    ]

    @staticmethod
    def space(types: list[str] = ['memory', 'abtio', 'pmdk'],
              transfer_managers: list[str] = ['__default__', 'pipeline'],
              paths: list[str] = [''],
              initial_size: int|None = 10485760,
              pipeline_num_pools: int|tuple[int,int] = 1,
              pipeline_num_buffers_per_pool: int|tuple[int,int] = 8,
              pipeline_first_buffer_size: int|tuple[int,int] = 1024,
              pipeline_buffer_size_multiplier: int|tuple[int,int] = 2,
              need_persistence: bool = True,
              **kwargs):
        from mochi.bedrock.config_space import (
                ConfigurationSpace,
                CategoricalOrConst,
                IntegerOrConst,
                Constant)
        storage_backends = [b for b in WarabiProviderSpec._backends if \
            ((not need_persistence) or b.is_persistent) and b.name in types]
        storage_types = [b.name for b in storage_backends]
        target_cs = ConfigurationSpace()
        target_cs.add(CategoricalOrConst('type', storage_types, default=storage_types[0]))

        if 'abtio' in storage_types:
            abtio_cs = ConfigurationSpace()
            abtio_cs.add(CategoricalOrConst('path', paths, default=paths[0]))
            target_cs.add_configuration_space(
                prefix='abtio', delimiter='.',
                configuration_space=abtio_cs,
                parent_hyperparameter={'parent': target_cs['type'], 'value': 'abtio'})

        if 'pmdk' in storage_types:
            pmdk_cs = ConfigurationSpace()
            pmdk_cs.add(CategoricalOrConst('path', paths, default=paths[0]))
            if initial_size is None:
                raise ValueError('initial_size but be provided if pmdk is a possible backend')
            pmdk_cs.add(Constant('initial_size', initial_size))
            target_cs.add_configuration_space(
                prefix='pmdk', delimiter='.',
                configuration_space=pmdk_cs,
                parent_hyperparameter={'parent': target_cs['type'], 'value': 'pmdk'})

        tm_cs = ConfigurationSpace()
        tm_cs.add(CategoricalOrConst('type', transfer_managers, default=transfer_managers[0]))

        if 'pipeline' in transfer_managers:
            pipeline_cs = ConfigurationSpace()
            pipeline_cs.add(IntegerOrConst('num_pools', pipeline_num_pools))
            pipeline_cs.add(IntegerOrConst('num_buffers_per_pool', pipeline_num_buffers_per_pool))
            pipeline_cs.add(IntegerOrConst('first_buffer_size', pipeline_first_buffer_size))
            pipeline_cs.add(IntegerOrConst('buffer_size_multiplier', pipeline_buffer_size_multiplier))
            tm_cs.add_configuration_space(
                prefix='pipeline', delimiter='.',
                configuration_space=pipeline_cs,
                parent_hyperparameter={'parent': tm_cs['type'], 'value': 'pipeline'})

        cs = ConfigurationSpace()
        cs.add_configuration_space(
            prefix='target', delimiter='.',
            configuration_space=target_cs)
        cs.add_configuration_space(
            prefix='transfer_manager', delimiter='.',
            configuration_space=tm_cs)

        def provider_config_resolver(config: 'Configuration', prefix: str) -> dict:
            target = {}
            target['type'] = config[f'{prefix}target.type']
            t = target['type']
            if t == 'memory':
                target['config'] = {}
            elif t == 'abtio':
                target['config'] = {
                    'create_if_missing': True,
                    'override_if_exists': True,
                    'path': config[f'{prefix}target.{t}.path']
                }
            elif t == 'pmdk':
                target['config'] = {
                    'create_if_missing_with_size': int(config[f'{prefix}target.{t}.initial_size']),
                    'override_if_exists': True,
                    'path': config[f'{prefix}target.{t}.path']
                }
            tm = {}
            tm['type'] = config[f'{prefix}transfer_manager.type']
            if tm['type'] == 'pipeline':
                p = f'{prefix}transfer_manager.pipeline.'
                tm['config'] = {
                    'num_pools': int(config[f'{p}num_pools']),
                    'num_buffers_per_pool': int(config[f'{p}num_buffers_per_pool']),
                    'first_buffer_size': int(config[f'{p}first_buffer_size']),
                    'buffer_size_multiplier': int(config[f'{p}buffer_size_multiplier'])
                }
            result = {
                'target': target,
                'transfer_manager': tm
            }
            return result

        kwargs['provider_config_space'] = cs
        kwargs['provider_config_resolver'] = provider_config_resolver
        kwargs['dependency_config_space'] = None
        kwargs['dependency_resolver'] = None

        provider_cs = ProviderSpec.space(type='warabi', **kwargs)
        return provider_cs

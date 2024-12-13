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
from mochi.bedrock.spec import (
        ProviderConfigSpaceBuilder,
        CS,
        Config,
        ProviderSpec)


@dataclass(frozen=True)
class BackendType:

    name: str
    is_persistent: bool = True



class WarabiSpaceBuilder:

    _backends = [
        BackendType(name='memory', is_persistent=False),
        BackendType(name='abtio'),
        BackendType(name='pmdk'),
    ]

    def __init__(self, *,
                 types: list[str] = ['memory', 'abtio', 'pmdk'],
                 paths: list[str] = [],
                 initial_size: int|None = 10485760,
                 need_persistence: bool = False,
                 transfer_managers: list[str] = ['__default__', 'pipeline'],
                 pipeline_num_pools: int|tuple[int,int] = 1,
                 pipeline_num_buffers_per_pool: int|tuple[int,int] = 8,
                 pipeline_first_buffer_size: int|tuple[int,int] = 1024,
                 pipeline_buffer_size_multiplier: int|tuple[int,int] = 2,
                 tags: list[str] = []):
        if need_persistence and len(paths) == 0:
            raise ValueError("Paths should be provided if database needs persistence")
        self.types = [b for b in WarabiSpaceBuilder._backends if \
                ((not need_persistence) or b.is_persistent) and \
                b.name in types]
        self.paths = paths
        self.tags = tags
        self.initial_size = initial_size
        self.transfer_managers = transfer_managers
        self.pipeline_num_pools = pipeline_num_pools
        self.pipeline_num_buffers_per_pool = pipeline_num_buffers_per_pool
        self.pipeline_first_buffer_size = pipeline_first_buffer_size
        self.pipeline_buffer_size_multiplier = pipeline_buffer_size_multiplier

    def set_provider_hyperparameters(self, configuration_space: CS) -> None:
        from mochi.bedrock.config_space import (
            CategoricalChoice,
            InCondition,
            IntegerOrConst,
            EqualsCondition,
            CategoricalOrConst)
        # add a pool dependency
        num_pools = configuration_space['margo.argobots.num_pools']
        configuration_space.add(CategoricalChoice('pool', num_options=num_pools))
        # add backend type
        hp_type = CategoricalOrConst('type', self.types)
        configuration_space.add(hp_type)
        # add path
        if len(self.paths) != 0:
            hp_path = CategoricalOrConst('path', self.paths)
            configuration_space.add(hp_path)
            path_if_persistent = InCondition(hp_path, hp_type, [t for t in self.types if t.is_persistent])
            configuration_space.add(path_if_persistent)
        # add transfer manager type
        hp_tm_type = CategoricalOrConst('tm.type', self.transfer_managers)
        configuration_space.add(hp_tm_type)
        # add the parameters for pipeline transfer manager, if necessary
        if 'pipeline' in self.transfer_managers:
            hp_pipeline_num_pools = IntegerOrConst('tm.pipeline.num_pools',
                                                   self.pipeline_num_pools)
            configuration_space.add(hp_pipeline_num_pools)
            pipeline_num_pools_cond = EqualsCondition(
                    hp_pipeline_num_pools, hp_tm_type, 'pipeline')
            configuration_space.add(pipeline_num_pools_cond)
            hp_pipeline_num_buffers_per_pool = IntegerOrConst('tm.pipeline.num_buffers_per_pool',
                                                              self.pipeline_num_buffers_per_pool)
            configuration_space.add(hp_pipeline_num_buffers_per_pool)
            pipeline_num_buffers_per_pool_cond = EqualsCondition(
                    hp_pipeline_num_buffers_per_pool, hp_tm_type, 'pipeline')
            configuration_space.add(pipeline_num_buffers_per_pool_cond)
            hp_pipeline_first_buffer_size = IntegerOrConst('tm.pipeline.first_buffer_size',
                                                           self.pipeline_first_buffer_size)
            configuration_space.add(hp_pipeline_first_buffer_size)
            pipeline_first_buffer_size_cond = EqualsCondition(
                    hp_pipeline_first_buffer_size, hp_tm_type, 'pipeline')
            configuration_space.add(pipeline_first_buffer_size_cond)
            hp_pipeline_buffer_size_multiplier = IntegerOrConst('tm.pipeline.buffer_size_multiplier',
                                                                self.pipeline_buffer_size_multiplier)
            configuration_space.add(hp_pipeline_buffer_size_multiplier)
            pipeline_buffer_size_multiplier_cond = EqualsCondition(
                    hp_pipeline_buffer_size_multiplier, hp_tm_type, 'pipeline')
            configuration_space.add(pipeline_buffer_size_multiplier_cond)


    def resolve_to_provider_spec(self, name: str, provider_id: int,
                                 config: Config, prefix: str) -> ProviderSpec:
        # parameter keys
        type_key = prefix + "type"
        path_key = prefix + "path"
        tm_type_key = prefix + "tm.type"
        # extract info for target
        backend = config[type_key]
        target_type = backend.name
        target_config = {}
        if path_key in config:
            target_config["path"] = config[path_key]
        if backend.name == "pmdk":
            target_config["create_if_missing_with_size"] = self.initial_size
            target_config["override_if_exists"] = True
        elif backend.name == "abtio":
            target_config["create_if_missing"] = True
            target_config["override_if_exists"] = True
        # extra info for transfer manager
        tm_type = config[tm_type_key]
        tm_config = {}
        if tm_type == "pipeline":
            tm_config = {
                "num_pools": config[prefix + "tm.pipeline.num_pools"],
                "num_buffers_per_pool": config[prefix + "tm.pipeline.num_buffers_per_pool"],
                "first_buffer_size": config[prefix + "tm.pipeline.first_buffer_size"],
                "buffer_size_multiplier": config[prefix + "tm.pipeline.buffer_size_multiplier"]
            }
        # final configuration
        cfg = {
            "target": {
                "type": target_type,
                "config": target_config
            },
            "transfer_manager": {
                "type": tm_type,
                "config": tm_config
            }
        }
        # dependencies
        dep = {
            "pool" : int(config[prefix + "pool"])
        }
        return ProviderSpec(name=name, type="warabi", provider_id=provider_id,
                            tags=self.tags, config=cfg, dependencies=dep)

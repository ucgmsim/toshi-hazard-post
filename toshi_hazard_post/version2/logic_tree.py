from typing import TYPE_CHECKING, Generator, List, Sequence
import copy
from operator import mul
from functools import reduce
from itertools import chain, filterfalse, product
from dataclasses import dataclass, asdict, field


if TYPE_CHECKING:
    from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree, SourceBranch, GMCMBranch
    import numpy.typing as npt

# this is a dataclass so that we can use asdict for the __repr__()
@dataclass
class HazardBranch:
    """
    A component branch of the combined (SRM + GMCM) logic tree comprised of an srm branch and a gmcm branch. The HazardComposite branch is the smallest unit necessary to create a hazard curve realization.

    Parameters:
        source_branch: the source
        gmcm_branch: the ground motion model
    """

    source_branch: 'SourceBranch'
    gmcm_branches: Sequence['GMCMBranch']
    weight: float = field(init=False)

    def __post_init__(self):
        self.weight = reduce(mul, [self.source_branch.weight] + [b.weight for b in self.gmcm_branches])

    def __repr__(self) -> str:
        return(repr(asdict(self)))

@dataclass
class HazardCompositeBranch:
    """
    A composite branch of the combined (SRM + GMCM) logic tree. A HazardCompositeBranch will have multiple sources and multiple ground motion models and is formed by taking all combinations of branches from the branch sets. The HazardComposite branch is an Iterable and will return HazardComponentBranch when iterated.

    Parameters:
        branches: the source-ground motion pairs that comprise the HazardCompositeBranch
    """

    branches: List[HazardBranch] = field(default_factory=list)
    weight: float = field(init=False)

    def __post_init__(self) -> None:
        self.weight = reduce(mul, [branch.weight for branch in self.branches])

    def __iter__(self) -> 'HazardCompositeBranch':
        self.__counter = 0
        return self
    
    def __next__(self) -> HazardBranch:
        if self.__counter >= len(self.branches):
            raise StopIteration
        else:
            self.__counter += 1
            return self.branches[self.__counter - 1]


# TODO: move to nzhsm_model?
class HazardLogicTree:
    """
    The combined (SRM + GMCM) logic tree needed to define the complete hazard model.

    Parameters:
        srm_logic_tree: the source (SRM) logic tree
        gmcm_logic_tree: the ground motion (GMCM) logic tree
    """

    def __init__(self, srm_logic_tree: 'SourceLogicTree', gmcm_logic_tree: 'GMCMLogicTree') -> None:
        self.srm_logic_tree = srm_logic_tree

        # remove the TRTs from the GMCM logic tree that are not in the SRM logic tree 
        # 1. find which TRTs are included in the source logic tree
        self.trts = set(chain(*[bs.tectonic_region_types for bs in self.srm_logic_tree.branch_sets]))

        # 2. make a copy of the gmcm logic tree. Elimanate any BranchSets with a TRT not included in the source tree
        self.gmcm_logic_tree = copy.deepcopy(gmcm_logic_tree)
        self.gmcm_logic_tree.branch_sets[:] = filter(
            lambda bs: bs.tectonic_region_type in self.trts, gmcm_logic_tree.branch_sets
        )

        self._n_composite_branches: int

    # TODO: is this better stored or as a generator?
    @property
    def composite_branches(self) -> Generator[HazardCompositeBranch, None, None]:
        """
        Yield the composite branches combining the SRM branches with the appropraite GMCM branches by matching tectonic region type

        Returns:
            composite_branches: the composite branches that make up all full realizations of the complete hazard logic tree
        """
        for srm_composite_branch, gmcm_composite_branch in product(
            self.srm_logic_tree.composite_branches,
            self.gmcm_logic_tree.composite_branches
        ):
            # for each srm component branch, find the matching GMCM branches (by TRT)
            hbranches = []
            for srm_branch in srm_composite_branch:
                trts = srm_branch.tectonic_region_types
                gmcm_branches = [branch for branch in gmcm_composite_branch if branch.tectonic_region_type in trts]
                hbranches.append(HazardBranch(source_branch=srm_branch, gmcm_branches=gmcm_branches))
            yield HazardCompositeBranch(hbranches)


    @property
    def n_composite_branches(self) -> int:
        if not self._n_composite_branches:
            self._n_composite_branches = len(list(self.composite_branches))
        return self._n_composite_branches

    @property
    def component_branches(self) -> Generator[HazardBranch, None, None]:
        """
        Yield the component branches (each SRM branch with all possible GMCM branch matches)

        Returns:
            component_branches: the component branches that make up the independent realizations of the logic tree
        """

        for srm_branch in self.srm_logic_tree:
            trts = srm_branch.tectonic_region_types
            branch_sets = [
                branch_set for branch_set in self.gmcm_logic_tree.branch_sets if branch_set.tectonic_region_type in trts
            ]
            for gmcm_branches in product(*[bs.branches for bs in branch_sets]):
                yield HazardBranch(source_branch=srm_branch, gmcm_branches=gmcm_branches)


    # TODO: is it better to make this a generator or return list and cast to np.array when using it?
    # Keep numpy types from poluting logic tree?  Would def want to do if this class is moved to nzshm_model
    @property
    def weights(self) -> Generator[float, None, None]:
        """
        The weights for every enumerated branch (srm + gmcm) of the logic tree.

        Parameters:
            logic_tree: the complete (srm + gmcm combined) logic tree

        Returns:
            weights: one dimensional array of branch weights
        """
        for composite_branch in self.composite_branches:
            yield composite_branch.weight

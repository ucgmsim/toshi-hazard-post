from typing import TYPE_CHECKING
import numpy as np

if TYPE_CHECKING:
    from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree
    import numpy.typing as npt

# TODO: move to nzhsm_model?
class HazardLogicTree:

    def __init__(srm_logic_tree: SourceLogicTree, gmcm_logic_tree: GMCMLogicTree) -> None:
        pass

    # TODO: is it better to make this a generator or return list and cast to np.array when using it?
    # Keep numpy types from poluting logic tree?  Would def want to do if this class is moved to nzshm_model
    @property
    def weights(self) -> 'npt.NDArray':
        """
        The weights for every enumerated branch (srm + gmcm) of the logic tree.

        Parameters:
            logic_tree: the complete (srm + gmcm combined) logic tree

        Returns:
            weights: one dimensional array of branch weights
        """
        weights = np.empty((self.num_branches, ))
        i = 0
        for branch in self.source_branches:
            weights_part = np.array(branch.gmcm_branch_weights) * branch.weight
            weights[i:i + len(weights_part)] = weights_part
            i += len(weights_part)
        return weights



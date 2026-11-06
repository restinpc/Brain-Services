"""Strict ordered masked reduction, reusable by pure numeric model providers."""
import numpy as np
from numba import njit
@njit(nogil=True,fastmath=False,cache=True)
def _reduce(t1,hits,weights,masks,dates,frame_ends,valid,end,current,target):
 stats=np.zeros((masks.shape[1],6),dtype=np.float64)
 for j in range(end):
  if valid[j]==0 or dates[j]==current or frame_ends[j]>target:continue
  for v in range(masks.shape[1]):
   if masks[j,v]==0:continue
   stats[v,0]+=t1[j]
   stats[v,1]+=hits[j]
   stats[v,2]+=1
   stats[v,3]+=t1[j]*weights[j]
   stats[v,4]+=hits[j]*weights[j]
   stats[v,5]+=weights[j]
 return stats
def strict_masked_reduce(t1,hits,weights,masks,dates,frame_ends,valid,end,current,target):
 n=len(t1)
 if not (0<=end<=n and masks.ndim==2 and masks.shape[0]==n and all(len(x)==n for x in [hits,weights,dates,frame_ends,valid])):
  raise ValueError('REDUCTION_SHAPE_BOUNDS')
 if masks.shape[1]>256 or n>1_000_000:raise ValueError('REDUCTION_MEMORY_BOUNDS')
 return _reduce(t1,hits,weights,masks,dates,frame_ends,valid,end,current,target)

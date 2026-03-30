# Count -> bucket by frequency -> scan from high to low
# for bottom k scan from low to high

from typing import List


class Solution:
    def topKFrequent(self, nums: List[int], k: int) -> List[int]:
       freq = {}
       for number in nums:
           freq[number] = freq.get(number,0) + 1
           
       buckets = [ [] for _ in range(len(nums) + 1)]
       
       for n, f in freq.items():
           buckets[f].append(n)
           
       res = []
       for f in range(len(buckets) - 1, 0, -1):
           for n in buckets[f]:
               res.append(n)
               if len(res) == k:
                   return res
               
sol = Solution()

print(sol.topKFrequent([1, 2, 2, 3, 3, 3], 2))
print(sol.topKFrequent([7, 7], 1))

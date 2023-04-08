# scratch pad for testing stuff

import sys
import re

txt: str = 'This is the code-based behind the Twitter account @ukpassportcheck. Nothing is hidden. I code in the open. You can see all the code here and select Actions tab to see the GitHub Actions in progress.'
final = re.sub('[^a-zA-Z ]', '', txt)
data = {'Python': [],
        'Java': [],
        'C': []}
final = final.split(' ')
print(final)

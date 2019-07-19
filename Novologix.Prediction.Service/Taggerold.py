
import nltk
from nltk import word_tokenize, pos_tag, ne_chunk
from nltk.chunk import conlltags2tree, tree2conlltags
from pprint import pprint
import sys
searchStr= sys.argv[1]


#from nltk.tokenize import MWETokenizer
#tokenizer = MWETokenizer()
#tokenizer.add_mwe(('Stage', 'IV'))
#test = tokenizer.tokenize('Something about the west wing'.split())


print('NTLK version: %s' % (nltk.__version__))

def fn_preprocess(art):
    art = str(art).replace("(","")
    art = str(art).replace(")","")
    art = str(art).replace("/","")
    art = str(art).replace(",","")
    art = str(art).replace(";","")
    art = nltk.word_tokenize(art)
    art = nltk.pos_tag(art)
    results = ne_chunk(art)
    return results

art_processed = fn_preprocess(searchStr)
#print(art_processed)

grammar = r"""
  #DISEASE: {<RB><VBG><NN>*}               # Chunk prepositions followed by NP
  NOUNFORMS: {<JJ>*<NN.*|CD>+}          # Chunk sequences of DT, JJ, NN
  #VP: {<VB.*><NP|PP|CLAUSE>+$}           Chunk verbs and their arguments
  #CLAUSE: {<NP><VP>}                        Chunk NP, VP
  """
cp = nltk.RegexpParser(grammar)
cs = cp.parse(art_processed)
#print(cs)

iob_tagged = tree2conlltags(cs)
#pprint(iob_tagged)
pstr=""
for j in iob_tagged:
    (text,ty,iob) = j
    if "B-NOUNFORMS" in iob:
        print(pstr)        
        pstr = ""
    
    if ty != "CC" and ty != "IN"  and ty != "DT" :
        pstr = pstr + " " + text

print(pstr)     
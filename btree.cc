#include <assert.h>
#include "btree.h"
#include <math.h>

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  // BTREE_OP_UPDATE
	  if(op==BTREE_OP_UPDATE){
		if((rc = b.SetVal(offset,value))) return rc;
    rc = b.Serialize(buffercache, node);
    return rc;
	  }
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}

ERROR_T BTreeIndex::InsertInternalRecursive(SIZE_T node, KEY_T key, VALUE_T value, SIZE_T newNode)
{
  BTreeNode b;
  BTreeNode bNew;
  ERROR_T rc;

  //If root node has no keys...
  if ((rc = b.Unserialize(buffercache, superblock.info.rootnode))) return rc;
  if (b.info.numkeys == 0)
  {
    rc = makeTree(b, key, value);
    return rc;
  }

  //If this is the first call before any recursion
  if(node == 0)
    //Find the leaf node that would contain key
    node = FindLeaf(key);

  //If the leaf node couldn't be found
  if(node == 0)
    return ERROR_INSANE;


  //Add key value pair to node (rhs if interior or root)
  if ((rc = InsertKeyValue(node, key, value, newNode, true))) return rc;

  //Get node
  if ((rc = b.Unserialize(buffercache, node))) return rc;

  //If leaf node & too full
  if(b.info.nodetype == BTREE_LEAF_NODE &&
    ceil((b.info.GetNumSlotsAsLeaf()*(2./3.))) <= b.info.numkeys)
  {
    //Create new node
    SIZE_T newLeafNode;
    if ((rc = AllocateNode(newLeafNode))) return rc;
    bNew = b;
    bNew.info.nodetype = BTREE_LEAF_NODE;
    bNew.info.numkeys = 0;
    if ((rc = bNew.Serialize(buffercache,newLeafNode))) return rc;
    //Split keys (including new key) and values evenly accross both nodes (splitting process specifc to leaf nodes)
    KEY_T splittingKey = SplitNode(node, newLeafNode);
    //Find parent of original node
    SIZE_T parent = FindParent(node);
    //Add new key (splitting key) and value (pointer to new node) to parent (recursion) (add to right hand side)
    if ((rc = InsertInternalRecursive(parent, splittingKey, VALUE_T((SIZE_T)0), newLeafNode))) return rc;
  }

  //If root or interior & too full
  else if((b.info.nodetype == BTREE_INTERIOR_NODE || b.info.nodetype == BTREE_ROOT_NODE) &&
    ceil((b.info.GetNumSlotsAsInterior()*(2./3.))) <= b.info.numkeys)
  {
    //Create new node
    SIZE_T newInteriorNode;
    if ((rc = AllocateNode(newInteriorNode))) return rc;
    bNew = b;
    bNew.info.nodetype = BTREE_INTERIOR_NODE;
    bNew.info.numkeys = 0;
    if ((rc = bNew.Serialize(buffercache,newInteriorNode))) return rc;
    //Split keys (including new key) and values evenly accross both nodes (splitting process specifc to internal nodes)
    KEY_T splittingKey = SplitNode(node, newInteriorNode);
    //If interior node
    if(b.info.nodetype == BTREE_INTERIOR_NODE)
    {
      //Find parent of original node
      SIZE_T parent = FindParent(node);
      //Add new key (splitting key) and value (new node) to parent (recursion) (add to right hand side)
      if ((rc = InsertInternalRecursive(parent, splittingKey, VALUE_T((SIZE_T)0), newInteriorNode))) return rc;
    }
    //If root node
    else
    {
      //Create a new root node
      SIZE_T newRootNode;
      if ((rc = AllocateNode(newRootNode))) return rc;
      bNew = b;
      bNew.info.nodetype = BTREE_ROOT_NODE;
      bNew.info.numkeys = 0;
      if ((rc = bNew.Serialize(buffercache,newRootNode))) return rc;
      superblock.info.rootnode = newRootNode;
      superblock.Serialize(buffercache,superblock_index);
      //Change type of old root node
      if ((rc = b.Unserialize(buffercache,node))) return rc;
      b.info.nodetype = BTREE_INTERIOR_NODE;
      if ((rc = b.Serialize(buffercache,node))) return rc;
      //Add new key (splitting key) and value (new node) to new root (no recursion) (add to right hand side)
      if ((rc = InsertKeyValue(newRootNode, splittingKey, VALUE_T((SIZE_T)0), newInteriorNode, true))) return rc;
      //Add value (old node) to new root (no recursion) (add to left hand side)
      if ((rc = InsertKeyValue(newRootNode, splittingKey, VALUE_T((SIZE_T)0), node, false))) return rc;
    }
  }

  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::makeTree(BTreeNode referenceNode, KEY_T key, VALUE_T value)
{
  BTreeNode bNew;
  ERROR_T rc;

  //Make new leaf node
  SIZE_T newLeafNode1;
  if ((rc = AllocateNode(newLeafNode1))) return rc;
  bNew = referenceNode;
  bNew.info.nodetype = BTREE_LEAF_NODE;
  bNew.info.numkeys = 0;
  if ((rc = bNew.Serialize(buffercache,newLeafNode1))) return rc;
  //Insert key/value into leaf node
  if ((rc = InsertKeyValue(newLeafNode1, key, value, (SIZE_T)0, false))) return rc;
  //Insert leaf node pointer into root (using input key as splitting key) (LHS)
  if ((rc = InsertKeyValue(superblock.info.rootnode, key, VALUE_T((SIZE_T)0), newLeafNode1, false))) return rc;

  //Make Second new leaf node
  SIZE_T newLeafNode2;
  if ((rc = AllocateNode(newLeafNode2))) return rc;
  bNew = referenceNode;
  bNew.info.nodetype = BTREE_LEAF_NODE;
  bNew.info.numkeys = 0;
  if ((rc = bNew.Serialize(buffercache,newLeafNode2))) return rc;
  //Insert leaf node pointer into root (using input key as splitting key) (RHS)
  if ((rc = InsertKeyValue(superblock.info.rootnode, key, VALUE_T((SIZE_T)0), newLeafNode2, true))) return rc;

  return ERROR_NOERROR;
}

SIZE_T BTreeIndex::FindLeaf(KEY_T key)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T currentNode;
  KEY_T testkey;

  //Set current node as the root of the tree
  currentNode = superblock.info.rootnode;
  if((rc= b.Unserialize(buffercache, currentNode))) return 0;

  while(b.info.nodetype != BTREE_LEAF_NODE)
  {

    // Scan through key/ptr pairs
    for (int offset=0; offset<b.info.numkeys; offset++)
    { 
      //Store key at offset in testkey
      if((rc = b.GetKey(offset, testkey))) return 0;
      //If testkey ==key or is less than key, we've found the correct pointer
      if (key<testkey || key==testkey)
      {
        //Get the correct pointer (to the node that's one level down)
        if((rc=b.GetPtr(offset,currentNode))) return 0;
        //Exit the for loop
        break;
      }
    }
    //Edge case: input key is larger than any keys in current node
    b.GetKey(b.info.numkeys-1,testkey);
    if(testkey < key)
    {
      //Get the correct pointer (to the node that's one level down)
      if((rc=b.GetPtr(b.info.numkeys,currentNode))) return 0;
    }

    //Get the node
    if((rc= b.Unserialize(buffercache, currentNode))) return 0;
  }

  //Return pointer to leaf node that would contain key
  return currentNode;
}

ERROR_T BTreeIndex::InsertKeyValue(SIZE_T node, KEY_T key, VALUE_T value, SIZE_T newNode, bool rhs)
{
  BTreeNode b;
  ERROR_T rc;
  KEY_T testkey = KEY_T((SIZE_T)0);

  //Get node from pointer
  if ((rc = b.Unserialize(buffercache, node))) return rc;
   
  //Find place in key list
  SIZE_T offset = 0;
  for (; offset<b.info.numkeys; offset++)
  { 
    rc=b.GetKey(offset,testkey);
    if (rc)
      return rc;
    if (testkey==key || key<testkey)
      break;
  }

  switch(b.info.nodetype)
  {
    //If leaf node
    case BTREE_LEAF_NODE:

      if (testkey==key)
      {
        return ERROR_CONFLICT;
      }
      //If the input key isn't less than any key in the node...
      if (offset == b.info.numkeys)
      {
        //Increment the number of keys
        b.info.numkeys+=1;
        superblock.info.numkeys++;

        //Add to the end of the list
        if ((rc = b.SetKey(offset,key))) return rc;
        if ((rc = b.SetVal(offset,value))) return rc;
      }

      else
      {
        //Increment the number of keys
        b.info.numkeys+=1;
        superblock.info.numkeys++;

        //Use input key-value as initial prev values
        KEY_T keyPrev = key;
        VALUE_T valuePrev = value;
        //Use key-value of testKey (1st key greater than input key) as curr values
        KEY_T keyCurr = testkey;
        VALUE_T valueCurr;
        rc = b.GetVal(offset,valueCurr);
        if (rc)
          return rc;

        //Insert input key-value pair and move all other pairs over
        for (SIZE_T i=offset; i<b.info.numkeys; i++)
        {
          //Store previous keys-value at current key-value location
          rc = b.SetKey(i,keyPrev);
          if (rc)
            return rc;
          rc = b.SetVal(i,valuePrev);
          if (rc)
            return rc;

          //Set current key-value to previous key-value
          keyPrev=keyCurr;
          valuePrev=valueCurr;

          //If we're not looking at the current end of the list...
          if(i < b.info.numkeys - 2)
          {
            //Get new current key-value at the next location
            rc=b.GetKey(i+1,keyCurr);
            if (rc)
              return rc;
            rc=b.GetVal(i+1,valueCurr);
            if (rc)
              return rc;
          }
        }
      }

      //Write changes back to the disk
      if ((rc=b.Serialize(buffercache,node))) return rc;
      return ERROR_NOERROR;

    //If root or interior node
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:

      if (testkey == key)
      {
        //If passed in value == 1, add to the rhs
        if(rhs)
        {
          if ((rc = b.SetPtr(offset+1,newNode))) return rc;
        }
        //If passed in value == 0, add to the lfs
        else
        {
          if ((rc = b.SetPtr(offset,newNode))) return rc;
        }
      }

      else if (offset == b.info.numkeys)
      {
        //Increment the number of keys
        b.info.numkeys+=1;
        superblock.info.numkeys++;

        //Add to the end of the list
        if ((rc = b.SetKey(offset,key))) return rc;
        //If passed in value == 1, add to the rhs
        if(rhs)
        {
          if ((rc = b.SetPtr(offset+1,newNode))) return rc;
        }
        //If passed in value == 0, add to the lfs
        else
        {
          if ((rc = b.SetPtr(offset,newNode))) return rc;
        }
      }

      //Insert new key in front of testKey and a new value
      //to the right of the new key (always to the right)
      else
      {
        //Increment the number of keys
        b.info.numkeys+=1;
        superblock.info.numkeys++;

        //Use input key-vlue as initial prev values
        KEY_T keyPrev = key;
        SIZE_T ptrPrev = newNode;
        //Use key-value of testKey (1st key greater than input key) as curr values
        KEY_T keyCurr = testkey;
        SIZE_T ptrCurr;
        rc = b.GetPtr(offset+1, ptrCurr); //Use value to the right of testkey
        if (rc)
          return rc;

        //Insert input key-value pair and move all other pairs over
        for (SIZE_T i=offset; i<b.info.numkeys; i++)
        {
          //Store previous keys-value at current key-value location
          rc = b.SetKey(i,keyPrev);
          if (rc)
            return rc;
          rc = b.SetPtr(i+1,ptrPrev);
          if (rc)
            return rc;

          //Set current key-value to previous key-value
          keyPrev = keyCurr;
          ptrPrev = ptrCurr;

          //If we're not looking at the current end of the list...
          if(i < b.info.numkeys - 2)
          {
            //Get new current key-value at the next location
            rc=b.GetKey(i+1,keyCurr);
            if (rc)
              return rc;
            rc=b.GetPtr(i+2,ptrCurr);
            if (rc)
              return rc;
          }
        }
      }

      //Write changes back to the disk
      if ((rc=b.Serialize(buffercache,node))) return rc;
      return ERROR_NOERROR;

    //If node of these types, error
    default:
      return ERROR_INSANE;
  }
}

KEY_T BTreeIndex::SplitNode(SIZE_T node, SIZE_T newNode)
{
  BTreeNode b;
  BTreeNode bNew;
  ERROR_T rc;
  KEY_T tempKey;
  SIZE_T tempPtr;
  VALUE_T tempVal;

  //Get the input node
  if((rc = b.Unserialize(buffercache, node))) return KEY_T((SIZE_T)0);
  //Get the input new node
  if((rc = bNew.Unserialize(buffercache, newNode))) return KEY_T((SIZE_T)0);


  //Get the total number of keys
  SIZE_T totalKeyNum = b.info.numkeys;
  //Get index of halfway point
  SIZE_T halfOffset = totalKeyNum/2;
  //Get splitting key (last key to be left in original node)
  KEY_T splittingKey;
  if((rc=b.GetKey(halfOffset-1,splittingKey))) return KEY_T((SIZE_T)0);

  //Initialize offset for new node
  SIZE_T iNew = 0;

  if(b.info.nodetype == BTREE_LEAF_NODE)
  {
    //Move key-value pairs from one node to the other
    for (SIZE_T i=halfOffset; i<totalKeyNum; i++)
    {
      //Get key-value pair from node
      if((rc=b.GetKey(i,tempKey))) return KEY_T((SIZE_T)0);
      if((rc=b.GetVal(i,tempVal))) return KEY_T((SIZE_T)0);

      //Insert key-value pair into new node
      bNew.info.numkeys++;
      if((rc=bNew.SetKey(iNew,tempKey))) return KEY_T((SIZE_T)0);
      if((rc=bNew.SetVal(iNew,tempVal))) return KEY_T((SIZE_T)0);

      //Increment the new node offset
      iNew++;
    }
    b.info.numkeys = halfOffset;
  }

  else if(b.info.nodetype == BTREE_INTERIOR_NODE || b.info.nodetype == BTREE_ROOT_NODE)
  {
    //Move key-value pairs from one node to the other
    for (SIZE_T i=halfOffset; i<totalKeyNum; i++)
    {
      //Get key-value pair from node
      if((rc=b.GetKey(i,tempKey))) return KEY_T((SIZE_T)0);
      if((rc=b.GetPtr(i,tempPtr))) return KEY_T((SIZE_T)0);

      //Insert key-value pair into new node
      bNew.info.numkeys++;
      if((rc=bNew.SetKey(iNew,tempKey))) return KEY_T((SIZE_T)0);
      if((rc=bNew.SetPtr(iNew,tempPtr))) return KEY_T((SIZE_T)0);

      //Increment the new node offset
      iNew++;
    }

    //If we are splitting an interior or root node, there will be one more pointer at the end
    //Get value from node
    if((rc=b.GetPtr(totalKeyNum,tempPtr))) return KEY_T((SIZE_T)0);
    //Insert value into new node
    if((rc=bNew.SetPtr(iNew,tempPtr))) return KEY_T((SIZE_T)0);

    b.info.numkeys = halfOffset;
  }

  //Write the input node to disk
  if((rc = b.Serialize(buffercache, node))) return KEY_T((SIZE_T)0);
  //Write the input new node to disk
  if((rc = bNew.Serialize(buffercache, newNode))) return KEY_T((SIZE_T)0);

  if((rc = b.Unserialize(buffercache, node))) return KEY_T((SIZE_T)0);

  return splittingKey;
}

SIZE_T BTreeIndex::FindParent(SIZE_T node)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T currentNode;
  SIZE_T prevNode;
  KEY_T testKey;
  KEY_T prevTestKey;
  KEY_T largest;
  KEY_T smallest;

  //Get the input node
  if((rc = b.Unserialize(buffercache, node))) return 0;

  //Get the largest and smallest keys in the input node
  b.GetKey(b.info.numkeys-1, largest);
  b.GetKey(0, smallest);

  //Start at the root of the tree
  currentNode = superblock.info.rootnode;
  if((rc = b.Unserialize(buffercache, currentNode))) return 0;

  while(b.info.nodetype != BTREE_LEAF_NODE)
  {

    //Initialize prevTestKey
    prevTestKey = KEY_T((SIZE_T)0);

    // Scan through key/ptr pairs
    for (int offset=0; offset<b.info.numkeys; offset++)
    { 
      //Store key at offset in testkey
      if((rc = b.GetKey(offset, testKey))) return 0;
      //If key range of input node is between this key and the previous key
      if ((largest < testKey || largest == testKey) && prevTestKey < smallest)
      {
        //Store the pointer to this node
        prevNode = currentNode;
        //Get the correct pointer (to the node that's one level down)
        if((rc=b.GetPtr(offset,currentNode))) return 0;
        //If the next node is the input node...
        if(currentNode == node)
          //Return a pointer to the prevous node
          return prevNode;
        //Get the node at that pointer
        if((rc = b.Unserialize(buffercache, currentNode))) return 0;
        //Exit the for loop
        break;
      }
      else
      {
        prevTestKey = testKey;
      }
    }
    //Edge case: input minimum is larger than any keys in current node
    b.GetKey(b.info.numkeys-1, testKey);
    if(testKey < smallest)
    {
      //Store the pointer to this node
      prevNode = currentNode;
      //Get the correct pointer (to the node that's one level down)
      if((rc=b.GetPtr(b.info.numkeys,currentNode))) return 0;
      //If the next node is the input node...
      if(currentNode == node)
        //Return a pointer to the prevous node
        return prevNode;
      //Get the node at that pointer
      if((rc = b.Unserialize(buffercache, currentNode))) return 0;
    }

    //Get the node form the disk 
    if((rc = b.Unserialize(buffercache, currentNode))) return 0;
  }

  //Return 0 to indicate no parent was found
  return 0;
}

static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME

  // Call the internal insert function with node == 0

  return InsertInternalRecursive(0, key, value, 0);
}
  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, (VALUE_T&)value);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::KeysInOrderRecursive(const SIZE_T &node, KEY_T testkey) const
{
  BTreeNode b;
  ERROR_T rc = ERROR_NOERROR;
  KEY_T prevkey = -1;
  SIZE_T ptr;
  SIZE_T offset;


  //Get node from pointer
  if ((rc = b.Unserialize(buffercache, node))) return rc;

  if (b.info.nodetype == BTREE_LEAF_NODE)
  {
    // Check to see that keys within leafnode are in correct order
    for (offset = 0; offset<b.info.numkeys; offset++) 
    {
      prevkey = testkey;
      if ((rc=b.GetKey(offset, testkey))) return rc;
      if (testkey < prevkey) {
        return ERROR_NOORDER;
      }
    }
  }
  else  // Interior or root node
  {  
    //Find place in key list
    for (offset = 0; offset<b.info.numkeys; offset++)
    { 
      if ((rc=b.GetPtr(offset, ptr))) return rc;
      if ((rc = KeysInOrderRecursive(ptr, testkey))) return rc;
    }
    if (b.info.numkeys>0) { 
      if ((rc=b.GetPtr(b.info.numkeys, ptr))) return rc;
      if ((rc = KeysInOrderRecursive(ptr, testkey))) return rc;
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
  }
  return rc;
}

ERROR_T BTreeIndex::KeysInOrder(const SIZE_T &node) const
{
  KEY_T testkey;
  ERROR_T rc;
  rc = KeysInOrderRecursive(node, testkey);
  if (rc) {
    std::printf("Returned error %d\n", rc);
  } else {
    std::printf("Keys are in order!\n");
  }
  return rc;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  ERROR_T rc;

  // Possible things to test
  // are the keys of the tree in order??
  if((rc = KeysInOrder(superblock.info.rootnode))) {return rc;}

  // Check if tree is at least half full
  if((rc = AtLeastHalfFullWrapper((SIZE_T)1))) return rc;

  return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::AtLeastHalfFullWrapper(const SIZE_T &node) const{
	std::printf("Start node:%d\n",node);
	ERROR_T rc = ERROR_NOERROR;
	float percentFull = AtLeastHalfFull(node);
	if(percentFull<.5){
		rc = ERROR_SIZE;
		std::printf("%f\n",percentFull);
	}
	return rc;
}
float BTreeIndex::AtLeastHalfFull(const SIZE_T &node) const{
  BTreeNode b;
  ERROR_T rc = ERROR_NOERROR;
  //KEY_T testkey;
  KEY_T prevkey = -1;
  SIZE_T ptr;
  SIZE_T offset;


  //Get node from pointer
  if ((rc = b.Unserialize(buffercache, node))) return rc;

  if (b.info.nodetype == BTREE_LEAF_NODE)
  {
    // count how many keys
  	return b.info.numkeys/(float)b.info.GetNumSlotsAsLeaf();
  }
  else  // Interior or root node
  {  
	float percentFull=0;
    //Find next node
    for (offset = 0; offset<b.info.numkeys; offset++)
    { 
	  
      if ((rc=b.GetPtr(offset, ptr))) return rc;
      percentFull += AtLeastHalfFull(ptr);
    }
    if (b.info.numkeys>0) { 
      if ((rc=b.GetPtr(b.info.numkeys, ptr))) return rc;
      percentFull += AtLeastHalfFull(ptr);
    } else {
      // There are no keys at all on this node, so nowhere to go
   		
		std::printf("Didn't want to get here...\n"); 
		std::printf("Node number: %d\nNode Type: %d\nNumKeys: %d\n",node,b.info.nodetype,b.info.numkeys);
		percentFull = 0.5; //Doing this won't affect whether our average is off
	}
	return percentFull/(b.info.numkeys+1);
  }
}


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_SORTED_KEYVAL);
  return os;
}





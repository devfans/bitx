package main

import (
  "testing"
  "bitx"
)

func TestConf(t *testing.T) {
  // test load/save block
  j := bitx.NewJob()
  j.Meta.Id = 0
  j.Meta.Name = "test.data"
  j.Prepare()
  _ = j.Meta.Dump()
  var i uint32
  for i = 0; i < j.Meta.Blocks ; i++ {
    j.AllocateBlock(i)
    block, _ := j.Blocks[i]
    j.LoadBlock(block)
  }

  j.Meta.Name = "test.data.copy"

  for i = 0; i < j.Meta.Blocks ; i++ {
    j.SaveBlock(i)
  }

  j1 := bitx.NewJob()
  j1.Meta.Id = 0
  j1.Meta.Name = "test.data.copy"
  j1.Prepare()
  _ = j1.Meta.Dump()

  if j1.Meta.GetHash() != j.Meta.GetHash() {
    t.Errorf("Hash diff %v --  %v", j1.Meta.GetHash(), j.Meta.GetHash())
  }
}



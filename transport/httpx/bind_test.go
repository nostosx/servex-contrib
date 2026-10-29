package httpx

import (
	"mime/multipart"
	"testing"
)

type RecursiveBindTarget struct {
	*RecursiveBindTarget
	Value string                `json:"value"`
	File  *multipart.FileHeader `json:"file"`
}

type MutualBindTarget struct {
	*MutualBindChild
	Value string `json:"value"`
}

type MutualBindChild struct {
	*MutualBindTarget
	Other string                `json:"other"`
	File  *multipart.FileHeader `json:"file"`
}

func TestBindRecursiveEmbedding(t *testing.T) {
	var dst RecursiveBindTarget
	if err := bindValues(&dst, map[string][]string{"value": {"bound"}}); err != nil {
		t.Fatal(err)
	}
	file := &multipart.FileHeader{Filename: "upload.txt"}
	if err := bindFiles(&dst, map[string][]*multipart.FileHeader{"file": {file}}); err != nil {
		t.Fatal(err)
	}
	if dst.Value != "bound" || dst.File != file {
		t.Fatalf("bound fields = %+v", dst)
	}
	if dst.RecursiveBindTarget != nil {
		t.Fatal("binding allocated a recursive embedded pointer")
	}
}

func TestBindMutualEmbedding(t *testing.T) {
	var dst MutualBindTarget
	if err := bindValues(&dst, map[string][]string{"value": {"root"}, "other": {"child"}}); err != nil {
		t.Fatal(err)
	}
	file := &multipart.FileHeader{Filename: "upload.txt"}
	if err := bindFiles(&dst, map[string][]*multipart.FileHeader{"file": {file}}); err != nil {
		t.Fatal(err)
	}
	if dst.Value != "root" || dst.MutualBindChild == nil || dst.Other != "child" || dst.File != file {
		t.Fatalf("bound fields = %+v", dst)
	}
	if dst.MutualBindTarget != nil {
		t.Fatal("binding allocated the recursive back reference")
	}
}

func TestBindSharedEmbeddedType(t *testing.T) {
	type Shared struct {
		Value string `json:"value"`
	}
	type Left struct {
		Shared
		First string `json:"first"`
	}
	type Right struct {
		Shared
		Second string `json:"second"`
	}
	var dst struct {
		Left
		*Right
	}
	if err := bindValues(&dst, map[string][]string{
		"value": {"shared"}, "first": {"left"}, "second": {"right"},
	}); err != nil {
		t.Fatal(err)
	}
	if dst.Left.Value != "shared" || dst.First != "left" || dst.Right == nil || dst.Second != "right" {
		t.Fatalf("bound fields = %+v", dst)
	}
}

package main

import (
	"testing"
)

func TestSizeHumanizer(t *testing.T) {
	// Values are humanized to the next magnitude only when it is greater than
	// 1000 or 2014 of the previous magnitude (base 10 or base 2)

	if testVal := sizeHumanizer(1000, false); testVal != "1000 bytes" {
		t.Errorf("Failed to calculate 1000 bytes. %s", testVal)
	}
	if testVal := sizeHumanizer(1024, true); testVal != "1024 bytes" {
		t.Errorf("Failed to calculate 1024 bytes. %s", testVal)
	}
	if testVal := sizeHumanizer(1001, false); testVal != "1.00 KB" {
		t.Errorf("Failed to calculate 1 base 10 kilobyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1025, true); testVal != "1.00 KB" {
		t.Errorf("Failed to calculate 1 base 2 kilobyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1000001, false); testVal != "1.00 MB" {
		t.Errorf("Failed to calculate 1 base 2 megabyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1048577, true); testVal != "1.00 MB" {
		t.Errorf("Failed to calculate 1 base 2 megabyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1000000001, false); testVal != "1.00 GB" {
		t.Errorf("Failed to calculate 1 base 2 gigabyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1073741825, true); testVal != "1.00 GB" {
		t.Errorf("Failed to calculate 1 base 2 gigabyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1000000000001, false); testVal != "1.00 TB" {
		t.Errorf("Failed to calculate 1 base 2 terabyte. %s", testVal)
	}
	if testVal := sizeHumanizer(1099511627777, true); testVal != "1.00 TB" {
		t.Errorf("Failed to calculate 1 base 2 terabyte. %s", testVal)
	}
}

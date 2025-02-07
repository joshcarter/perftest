package main

import (
	"context"
)

type Runner interface {
	Run(context.Context)
}

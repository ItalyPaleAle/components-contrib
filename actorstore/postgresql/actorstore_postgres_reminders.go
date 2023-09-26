/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"context"

	"github.com/dapr/components-contrib/actorstore"
)

func (p *PostgreSQL) GetReminder(ctx context.Context, req actorstore.ReminderRef) (actorstore.GetReminderResponse, error) {
	panic("unimplemented")
}

func (p *PostgreSQL) CreateReminder(ctx context.Context, req actorstore.CreateReminderRequest) error {
	panic("unimplemented")
}

func (p *PostgreSQL) DeleteReminder(ctx context.Context, req actorstore.ReminderRef) error {
	panic("unimplemented")
}

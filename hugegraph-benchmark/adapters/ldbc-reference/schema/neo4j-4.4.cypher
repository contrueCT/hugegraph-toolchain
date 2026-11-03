// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Neo4j 4.4.24 only. Owned full-SF100 reference DB, after strict import.
// Preserves upstream schema; adds Place/TagClass ID uniqueness for full verification.
// Run once under the admitted schema writer, before the read-only verification lease.
// Partial failure requires inspection; do not replay this file automatically.

CREATE CONSTRAINT hgb_reference_City_id_unique FOR (n:City) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Comment_id_unique FOR (n:Comment) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Country_id_unique FOR (n:Country) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Forum_id_unique FOR (n:Forum) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Message_id_unique FOR (n:Message) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Organisation_id_unique FOR (n:Organisation) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Person_id_unique FOR (n:Person) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Post_id_unique FOR (n:Post) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Tag_id_unique FOR (n:Tag) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_Place_id_unique FOR (n:Place) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT hgb_reference_TagClass_id_unique FOR (n:TagClass) REQUIRE n.id IS UNIQUE;

CREATE BTREE INDEX hgb_reference_Country_name FOR (n:Country) ON (n.name);
CREATE BTREE INDEX hgb_reference_Message_creationDate FOR (n:Message) ON (n.creationDate);
CREATE BTREE INDEX hgb_reference_Person_firstName FOR (n:Person) ON (n.firstName);
CREATE BTREE INDEX hgb_reference_Post_creationDate FOR (n:Post) ON (n.creationDate);
CREATE BTREE INDEX hgb_reference_Tag_name FOR (n:Tag) ON (n.name);
CREATE BTREE INDEX hgb_reference_TagClass_name FOR (n:TagClass) ON (n.name);

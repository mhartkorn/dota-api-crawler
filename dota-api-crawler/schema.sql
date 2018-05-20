CREATE TABLE "matches" (
	`matchId`	INTEGER NOT NULL UNIQUE,
	`httpStatusCode` INTEGER NOT NULL,
	`errorMessage` TEXT DEFAULT NULL,
	`rawJson` TEXT NOT NULL,
	`private` INTEGER,
	`notFound` INTEGER,
	PRIMARY KEY(`matchId`)
);

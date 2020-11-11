## Update:

Fixed a bug where the obj.tables were being assigned incorrect name attributes preventing queries
to the corresponding database table. Added a Selector class, which is now called by a Table.select 
method which adds the appropiate attributes upon instantiation.

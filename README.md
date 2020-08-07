# guid-resolver-test-client

GUID resolver warm-up
---------------------

To use warmup using predefined contexts place newline sepearated context names
to local file, "./preprod_contexts.txt"

Setup HTTP connection string to GUID Resolver host/port in code
    
    com.thomsonreuters.ellis.feed.guids.GuidResolverWarmupSender.HOST

And run main method
    
    com.thomsonreuters.ellis.feed.guids.GuidResolverWarmupSender.main()'
    
By default batches of 1000 contexts will be sent to specified resolver.

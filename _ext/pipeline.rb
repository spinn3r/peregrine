
Awestruct::Extensions::Pipeline.new do

  extension Awestruct::Extensions::Posts.new( '/blog' ) 
  extension Awestruct::Extensions::Paginator.new(:posts, '/blog/index', :per_page => 5 )
  extension Awestruct::Extensions::Atomizer.new(:posts, '/blog/index.atom')

  helper Awestruct::Extensions::GoogleAnalytics

end


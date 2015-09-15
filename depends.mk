################################################################
# Note: Edit the variable below to help find your own package
#       that tera depends on.
#       If you build tera using build.sh or travis.yml, it will
#       automatically config this for you.
################################################################

BOOST_INCDIR=../thirdparty/boost_1_57_0

################################################################
# Note: No need to modify things below.
################################################################

DEPS_INCPATH = -I$(BOOST_INCDIR)

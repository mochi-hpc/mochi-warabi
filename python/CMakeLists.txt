

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

install (DIRECTORY mochi/warabi
         DESTINATION lib/python${PY_VERSION}/site-packages/mochi)

MACRO (FindRTM)

IF(CMAKE_SYSTEM_NAME MATCHES "Linux")
    EXEC_PROGRAM(cat ARGS "/proc/cpuinfo" OUTPUT_VARIABLE CPUINFO)

    STRING(REGEX REPLACE "^.*(rtm).*$" "\\1" RTM_THERE ${CPUINFO})
    STRING(COMPARE EQUAL "rtm" "${RTM_THERE}" RTM_TRUE)
    IF (RTM_TRUE)
        set(RTM_FOUND true CACHE BOOL "RTM available on host")
    ELSE (RTM_TRUE)
        set(RTM_FOUND false CACHE BOOL "RTM available on host")
    ENDIF (RTM_TRUE)
ELSE(CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(RTM_FOUND   false  CACHE BOOL "RTM available on host")
ENDIF(CMAKE_SYSTEM_NAME MATCHES "Linux")

ENDMACRO(FindRTM)
#!/usr/bin/env bash
# curl -XDELETE localhost:10101/index/dm

# curl localhost:10101/index/dm \
#      -X POST

# curl localhost:10101/index/dm/field/date \
#      -X POST \
#      -d '{"options":{"type": "time", "timeQuantum": "YMD" }}'


FIELDS=$(cat <<-END
date
did
app_id
event_id
event_type
session_id
package_name
product_id
app_ver
location_cd
device_maker
device_name
first_open_time
os_language
os
os_ver
language_cd
location_name
sys_prop1x
sys_prop1x
sys_prop2x
sys_prop3x
sys_prop4x
sys_prop5x
sys_prop6x
sys_prop7x
sys_prop8x
sys_prop9x
sys_prop10x
user_prop1x
user_prop2x
user_prop3x
user_prop4x
user_prop5x
user_prop6x
user_prop7x
user_prop8x
user_prop9x
user_prop10x
customize_prop1x
customize_prop2x
customize_prop3x
customize_prop4x
customize_prop5x
customize_prop6x
customize_prop7x
customize_prop8x
customize_prop9x
customize_prop10x
customize_prop11x
customize_prop12x
customize_prop13x
customize_prop14x
customize_prop15x
customize_prop16x
customize_prop17x
customize_prop18x
customize_prop19x
customize_prop20x
customize_prop21x
customize_prop22x
customize_prop23x
customize_prop24x
customize_prop25x
customize_prop26x
customize_prop27x
customize_prop28x
customize_prop29x
customize_prop30x
END
)

list:each() {
    local list=$1
    local func=${2:-echo}

    [ ! -z "$list" ] && while IFS= read -r line; do
        $func "$line"
    done << EOF
$list
EOF
}

list:each "$FIELDS"

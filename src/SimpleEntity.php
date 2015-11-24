<?php

namespace Assertis\SimpleDatabase;

use JsonSerializable;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
abstract class SimpleEntity implements JsonSerializable
{
    /**
     * @return array
     */
    abstract protected function getAsArray();

    /**
     * @param bool $isJson
     * @return array
     */
    public function toArray($isJson = false)
    {
        $out = [];
        foreach ($this->getAsArray() as $key => $value) {
            if ($isJson && is_object($value) && method_exists($value, 'jsonSerialize')) {
                $out[$key] = $value->jsonSerialize();
            } elseif (is_object($value) && method_exists($value, 'toArray')) {
                $out[$key] = $value->toArray();
            } else {
                $out[$key] = $value;
            }
        }
        return $out;
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray(true);
    }
}
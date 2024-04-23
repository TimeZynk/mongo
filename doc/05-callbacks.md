# Call-back Functions

There are two ways to automate interjection through call-back functions during calls:
* document hooks,
* preemptive guards.

## Hooks

Hooks allow you to make operations on documents (queries and payloads) during an API call. The main intention is to allow schema conversions to and from MongoDB.

```Clojure
(require '[com.timezynk.mongo.hooks :refer [with-hooks]])

(with-hooks {:write #(rename-keys % {:a :b})
             :read  #(rename-keys % {:b :a})}
  (fetch-and-update-one! :coll
                         {:a 1}
                         {:$set {}}))
```

## Guards

Guards allow you to add checks and operations before an API call is made. Inserts, updates, replacements each have their own guard.

Each guard has a default behaviour.

### Insert

text

### Update

text

### Replace

text

### Early Return

text

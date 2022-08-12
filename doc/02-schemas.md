# Schemas

When creating a collection, an optional schema can be added:

```Clojure
(require '[com.timezynk.mongo :as m])
(require '[com.timezynk.mongo.schema :as s])

(m/create-colection! :users :schema {:name (s/string)
                                     :year (s/integer :min 1900 :max 9999)
                                     :gender (s/string :in ["F" "M" "O"])
                                     :address (s/map {:street (s/string)
                                                      :number (s/number)}
                                               :optional? true)
                                     :reg-no (s/string :optional? true :regex "[A-Z]{3}\\d{3}")})
```

BEWARE! The schema can only be added on creation and cannot be altered afterwards.

In the example above, the collection `:user` gets a schema, stating that documents must have &ndash; and only have &ndash; the following fields:

* `:name` This field is required and must be a string.
* `:year` This field is required and must be an integer between 1900 and 9999 (inclusive).
* `:gender` This field is required and must be one of the strings in the array.
* `:address` This field is optional and must be a map with the approved fields. While the map is optional, the contained fields are not, should the map be added to the document.
* `:reg-no` This field is optional and a string, but must comply to a regular expression.

Any document will be validated for both inserts and updates. A violation will cause the Java driver to throw an exception.

## Custom validation

If more, or different, fidelity is required, custom validation can be added when creating the collection. For example, let's say that users don't need both an address and reg-no, but must have at least one:

```Clojure
(m/create-collection! :users :validation {:$or [{:address {:$ne nil}}
                                                {:reg-no {:$ne nil}}]})
```

Note: `{:$ne null}` and `{:$exists 1}` work equally well if a schema was defined, since `null` values are disallowed in that case.
